from enum import Enum
import pickle
import time
import signal
import sys
import logging
import json

import gevent
import gevent.pool
import gevent.queue
from gevent.lock import BoundedSemaphore
import yaml
import zmq.green as zmq

from elastic.benchmark import query
from elastic.benchmark import config


class BenchmarkConfig(config.BaseConfig):

    CONTROLLER_HOST = "controller.host"
    CONTROLLER_COMMANDER_PORT = "controller.commander.port"
    CONTROLLER_JOB_QUEUE_PORT = "controller.job_queue.port"
    CONTROLLER_REPORT_QUEUE_PORT = "controller.report_queue.port"
    CONTROLLER_MANAGER_PORT = "controller.manager.port"

    DRIVER_NUM_WORKER = "driver.num_workers"
    DRIVER_QUERY_TIMEOUT = "driver.query_timeout"
    DRIVER_MANUAL_ROUTING = "driver.manual_routing"

    WORKLOAD_NUM_QUERIES = "workload.num_queries"
    WORKLOAD_APPLICATIONS = "workload.applications"


class SendProtocolHeader(Enum):
    admin_start = 0
    admin_stop = 1
    admin_status = 2
    admin_echo = 3
    admin_update_workload = 4
    admin_update_routing_table = 5

    membership_heartbeat = 11
    membership_register = 12
    membership_ready = 13
    membership_retrieve_routing_table = 14

    workload_submit = 21
    workload_status = 22
    workload_report = 23
    workload_statistics = 24
    workload_timeline_completion = 25
    workload_timeline_failure = 26
    workload_download = 27

    report_done = 31

    routing_table_upload = 41


class ReceiveProtocolHeader(Enum):
    successful = 0
    failed = 1


class BenchmarkProtocol:
    def __init__(self, header, payloads=None):
        self.header = header
        self.payloads = payloads


class BenchmarkNode:

    def __init__(self, config_path):
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
        self.logger.info("load benchmark configuration from {}".format(config_path))
        self.config = BenchmarkConfig.parse_file(config_path)
        self.context = zmq.Context()
        self.runner_group = gevent.pool.Group()

    def stop(self):
        raise Exception("stop method is not implemented yet")

    def update_workload(self, workload):
        raise Exception("update_workload is not implemented yet")

    def echo(self):
        raise Exception("echo is not implemented yet")

    def monitor(self):
        while True:
            self.logger.info("monitoring...")
            for runner in self.runner_group.greenlets:
                if not bool(runner):
                    raise Exception("Abnormal greenlet found")
            gevent.sleep(5)


class BenchmarkController(BenchmarkNode):

    class JobQueue:

        def __init__(self, sender):
            self.sender = sender
            self.counter = 0

        def enqueue(self, workload_item):
            self.sender.send_pyobj(workload_item)
            self.counter += 1

    class WorkloadRecord:
        def __init__(self, workload_id, workload):
            self.time_start = None
            self.time_last_report = None
            self.workload_id = workload_id
            self.workload = workload
            self.query_count = 0
            self.num_finished_jobs = 0
            self.ready_drivers = 0
            self.dispatch_completed = False
            self.counter_success = 0
            self.counter_failure = 0
            self.statistics = {}
            self.timeline_completion = {}
            self.timeline_failure = {}
            self.logger = logging.getLogger(__name__)
            self.statistics_lock = BoundedSemaphore(1)

        def start(self):
            self.logger.info("* jobs started")
            self.time_start = time.time()

        def get_workload(self):
            return self.workload

        def add_workload(self, num_queries):
            self.query_count += num_queries

        def completed_dispatch(self):
            self.logger.info("@ job dispatch completed")
            self.dispatch_completed = True

        def report_completion(self, report):
            self.time_last_report = time.time()
            self.num_finished_jobs += 1
            item_id = report['item']
            report.pop('item', None)
            self.statistics_lock.acquire()
            before_length = len(self.statistics)
            self.statistics[item_id] = report
            #self.logger.info("on report: {}, before={}, after={}".format(item_id, before_length, len(self.statistics)))
            self.statistics_lock.release()
            timeslot = int(self.time_last_report - self.time_start) + 1
            if report['success']:
                self.counter_success += 1
                if timeslot not in self.timeline_completion:
                    self.timeline_completion[timeslot] = 0
                self.timeline_completion[timeslot] += 1
            else:
                self.counter_failure += 1
                if timeslot not in self.timeline_failure:
                    self.timeline_failure[timeslot] = 0
                self.timeline_failure[timeslot] += 1


        def is_completed(self):
            """jobs may all complete before dispatch finish"""
            self.logger.info("# dispatch completed: %s", self.dispatch_completed)
            self.logger.info("@ num_queries={}, num_finished_jobs={}".format(self.query_count, self.num_finished_jobs))
            return self.dispatch_completed and (self.query_count == self.num_finished_jobs)

        def is_started(self):
            return self.time_start is not None

        def get_report(self):
            return {
                "num_finished_jobs": self.num_finished_jobs,
                "num_successful_jobs": self.counter_success,
                "num_failed_jobs": self.counter_failure,
                "elapsed_time": self.time_last_report - self.time_start if self.is_completed() else time.time() - self.time_start
            }

        def get_statistics(self):
            self.logger.info("## total reported jobs: {}".format(len(self.statistics)))
            return self.statistics

        def get_timeline_completion(self):
            return self.timeline_completion

        def get_timeline_failure(self):
            return self.timeline_failure

    class StatisticsCollectorProtocol:
        def __init__(self, controller):
            self.controller = controller
            self.logger = logging.getLogger(__name__)

        def _receive(self):
            return self.controller.result_receiver.recv_pyobj()

        def process(self):
            report = self._receive().payloads
            self.controller.current_workload_record.report_completion(report)
            self.logger.debug(report)

    class NodeStatusRecord:
        def __init__(self):
            self.status = {}

        def update_status(self, driver_id):
            self.status[driver_id] = time.time()

        def print_status(self):
            for (driver_id, update_time) in self.status.items():
                print("{}: {:.1f}".format(driver_id, update_time))

    class CommandReceiverProtocol:
        def __init__(self, controller):
            self.logger = logging.getLogger(__name__)
            self.controller = controller
            self.logger.info("[CommandReceiverProtocol] initing...")
            try:
                self.router = {
                    SendProtocolHeader.membership_register: self.register,
                    SendProtocolHeader.membership_heartbeat: self.heartbeat,
                    SendProtocolHeader.membership_ready: self.ready,
                    SendProtocolHeader.membership_retrieve_routing_table: self.retrieve_routing_table,
                    SendProtocolHeader.admin_status: self.status,
                    SendProtocolHeader.admin_stop: self.stop,
                    SendProtocolHeader.workload_submit: self.workload_submit,
                    SendProtocolHeader.workload_report: self.workload_report,
                    SendProtocolHeader.workload_timeline_completion: self.workload_timeline_completion,
                    SendProtocolHeader.workload_timeline_failure: self.workload_timeline_failure,
                    SendProtocolHeader.workload_statistics: self.workload_statistics,
                    SendProtocolHeader.workload_status: self.workload_status,
                    SendProtocolHeader.routing_table_upload: self.routing_table_upload,
                }
            except Exception as e:
                self.logger.exception("failed to initialize the router mapping")
                import traceback
                traceback.print_exc()
            self.logger.info("[CommandReceiverProtocol] done...")

        def process(self):
            #self.logger.info("commander is waiting for command")
            cmd_protocol = self.controller.commander_socket.recv_pyobj()
            #self.logger.info("cmd=", cmd_protocol.header)
            #self.logger.info("payload=", cmd_protocol.payloads)
            reply_protocol = None
            try:
                result = self.router[cmd_protocol.header](cmd_protocol.payloads)
                #self.logger.info("result=", result)
                reply_protocol = BenchmarkProtocol(ReceiveProtocolHeader.successful, result)
            except Exception as e:
                self.logger.exception('Failed to process command')
                import traceback
                traceback.print_exc()
                reply_protocol = BenchmarkProtocol(ReceiveProtocolHeader.failed)
            self.controller.commander_socket.send_pyobj(reply_protocol)

        def register(self, param):
            self.logger.info("# register...")
            self.controller.started_drivers += 1
            return str(self.controller.started_drivers)

        def ready(self, param):
            print("@ report ready")
            self.controller.ready_drivers += 1

        def retrieve_routing_table(self, param):
            self.logger.info("# retrieve_routing_table")
            return self.controller.routing_table

        def heartbeat(self, driver_id):
            # print("received heartbeat from ", driver)
            self.controller.node_status_record.update_status(driver_id)

        def status(self, workload_id):
            return self.controller.node_status_record.status

        def stop(self, param):
            stop_protocol = BenchmarkProtocol(SendProtocolHeader.admin_stop)
            self.controller.manager_publisher.send_pyobj(stop_protocol)
            gevent.spawn_later(1, self.controller.stop)

        def routing_table_upload(self, routing_table):
            self.logger.info("@ routing table=%s", json.dumps(routing_table, indent=4))
            self.controller.routing_table = routing_table
            # sync with all driver nodes
            self.controller.manager_publisher.send_pyobj(BenchmarkProtocol(SendProtocolHeader.admin_update_routing_table))

        def workload_submit(self, workload):
            self.logger.info("@ workload=%s", workload)
            if (self.controller.current_workload_record is not None) and (not self.controller.current_workload_record.is_completed()):
                raise Exception("need to wait the current workload to complete")
            new_workload_id = len(self.controller.workload_db)
            self.controller.current_workload_record = BenchmarkController.WorkloadRecord(new_workload_id, workload)
            self.controller.workload_db[str(new_workload_id)] = self.controller.current_workload_record
            # here sync with all driver nodes
            echo_protocol = BenchmarkProtocol(SendProtocolHeader.admin_echo)
            self.controller.manager_publisher.send_pyobj(echo_protocol)
            self.controller.runner_group.spawn(self.controller.dispatcher)
            return str(new_workload_id)

        def workload_report(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_report()

        def workload_statistics(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_statistics()

        def workload_timeline_completion(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_timeline_completion()

        def workload_timeline_failure(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_timeline_failure()

        def workload_status(self, workload_id):
            return self.controller.workload_db[str(workload_id)].is_completed()

        def workload_download(self, workload_id):
            return self.controller.workload_db[str(workload_id).get_workload()]

    def __init__(self, config_path):
        super(BenchmarkController, self).__init__(config_path)
        self.logger.info("Controller initing")

        self.num_drivers = len(self.config.get_drivers())
        # self.num_queries = self.config.get_config("workload")["num_quries"]
        self.job_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)
        self.result_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)
        self.node_status_record = BenchmarkController.NodeStatusRecord()
        self.routing_table = {}

        self.ready_drivers = 0
        self.started_drivers = 0

        self.workload_db = {}
        self.current_workload_record = None

    def start(self):
        self.logger.info("Controller starting")

        self.runner_group.spawn(self.monitor)
        self.runner_group.spawn(self.commander)
        # self.runner_group.spawn(self.manager)
        self.runner_group.spawn(self.collector)

        self.logger.info("Controller started")

        self.runner_group.join()

    def stop(self):
        self.runner_group.kill()

    def update_workload(self, workload):
        pass

    def commander(self):
        self.logger.info("initializing the commander")
        self.commander_socket = self.context.socket(zmq.REP)
        self.commander_socket.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)))
        self.manager_publisher = self.context.socket(zmq.PUB)
        self.manager_publisher.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT)))

        command_protocol = BenchmarkController.CommandReceiverProtocol(self)

        self.logger.info("completed the commander initialization")
        while True:
            command_protocol.process()

    def dispatcher(self):
        self.logger.info('dispatcher...')
        job_sender = self.context.socket(zmq.PUSH)
        job_sender.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)))
        job_queue = BenchmarkController.JobQueue(job_sender)

        # wait for all drivers to start
        while self.ready_drivers < self.num_drivers:
            self.logger.info("# drivers: ready={}, total={}".format(self.ready_drivers, self.num_drivers))
            gevent.sleep(1)

        # start the counter
        self.current_workload_record.start()

        # start to dispatch workload - num_queries
        current_workload = self.current_workload_record.get_workload()

        for t, workload_items in current_workload.next():
            self.logger.info("current time: {}".format(t))
            self.logger.info("current workload items: {}".format(len(workload_items)))
            self.current_workload_record.add_workload(len(workload_items))
            for workload_item in workload_items:
                job_queue.enqueue(workload_item)
            self.logger.info("Queue counter: %s", job_queue.counter)
            gevent.sleep(1)

        self.current_workload_record.completed_dispatch()

        #job_sender.close()  # why close??

    def collector(self):
        self.logger.info('collector...')
        self.result_receiver = self.context.socket(zmq.PULL)
        self.result_receiver.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)))

        reporter_protocol = BenchmarkController.StatisticsCollectorProtocol(self)

        while True:
            reporter_protocol.process()


class BenchmarkDriver(BenchmarkNode):

    def __init__(self, config_path):
        super(BenchmarkDriver, self).__init__(config_path)
        self.logger.info("Driver initing")
        self.num_workers = self.config.lookup_config(BenchmarkConfig.DRIVER_NUM_WORKER)
        self.query_timeout = self.config.lookup_config(BenchmarkConfig.DRIVER_QUERY_TIMEOUT, 30)
        self.manual_routing_enabled = self.config.lookup_config(BenchmarkConfig.DRIVER_MANUAL_ROUTING, False)
        self.logger.info("manual_routing_enabled:", self.manual_routing_enabled)
        self.worker_pool = gevent.pool.Pool(self.num_workers)
        self.worker_queue = gevent.queue.Queue()
        self.workload = None
        self.routing_table = {}
        self.routing_table_indexer = {}
        self.routing_table_lock = BoundedSemaphore(1)

    def start(self):
        self.logger.info("Driver starting")

        # retrieve driver id first
        self._register()

        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)))

        for i in range(self.num_workers):
            self.worker_pool.spawn(self.worker, i)

        self.runner_group.spawn(self.retriever)
        self.runner_group.spawn(self.manager)
        self.runner_group.spawn(self.monitor)
        self.runner_group.spawn(self.heartbeater)

        self.logger.info("Driver started")
        self.runner_group.join()


    def stop(self):
        self.worker_pool.kill()
        self.runner_group.kill()

    def update_workload(self, workload):
        self.workload = workload
        BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).ready(self.driver_id)

    def echo(self):
        BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).ready(self.driver_id)

    def _register(self):
        self.driver_id = BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).register()

    def _retrieve_routing_table(self):
        self.logger.info('retrieving routing table')
        self.routing_table = BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).retrieve_routing_table()
        self.logger.info('updated routing table: {}'.format(len(self.routing_table)))
        self.routing_table_indexer = {}
        for query_name, endpoints in self.routing_table.items():
            self.routing_table_indexer[query_name] = 0

    def heartbeater(self):
        self.heartbeater_socket = self.context.socket(zmq.REQ)
        self.heartbeater_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)))
        heartbeat_protocol = BenchmarkSenderProtocol(self.heartbeater_socket)
        while True:
            heartbeat_protocol.heartbeat(self.driver_id)
            gevent.sleep(5)

    def manager(self):
        # TODO: the function should move to only the driver nodes
        self.manager_subscriber = self.context.socket(zmq.SUB)
        self.manager_subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.manager_subscriber.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(
            BenchmarkConfig.CONTROLLER_MANAGER_PORT)))
        while True:
            cmd_protocol = self.manager_subscriber.recv_pyobj()
            self.logger.info("manager: %s", cmd_protocol.header)
            if cmd_protocol.header is SendProtocolHeader.admin_stop:
                self.stop()
            elif cmd_protocol.header is SendProtocolHeader.admin_update_workload:
                self.update_workload(cmd_protocol.payloads)
            elif cmd_protocol.header is SendProtocolHeader.admin_echo:
                self.echo()
            elif cmd_protocol.header is SendProtocolHeader.admin_update_routing_table:
                self._retrieve_routing_table()

    def retriever(self):
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)))

        while True:
            #if self.worker_queue.qsize() > self.worker_pool.size * 10:
            #    self.logger.info("@ too much job, yield to others")
            #    gevent.sleep(1)
            #self.logger.info("waiting for message")
            workload_item = self.receiver.recv_pyobj()
            # dirty hack
            workload_item.queue_timestamp = time.time()
            self.worker_queue.put(workload_item)
            #self.logger.info("current queue size: {}".format(self.worker_queue.qsize()))

    def worker(self, worker_id):
        session = query.new_session()
        reporter_procotol = BenchmarkReporterProtocol(worker_id, self.sender)
        while True:
            worker_item = self.worker_queue.get()
            self.logger.info("idle workers: {}, queue lengths: {}".format(self.worker_pool.free_count(), self.worker_queue.qsize()))
            self.logger.info("worker {} is processing roxie query {}".format(worker_id, worker_item.wid))
            if self.manual_routing_enabled:
                try:
                    assigned_endpoint = self._select_endpoint(worker_item.query_name)
                    self.logger.info('from {} to {}'.format(worker_item.endpoint, assigned_endpoint))
                    worker_item.endpoint = assigned_endpoint
                except:
                    self.logger.exception('unable to select endpoint')
            self.logger.info(worker_item.endpoint, worker_item.query_name, worker_item.query_key, worker_item.key)
            start_timestamp = time.time()
            # Hack to add routing table support
            # need a global index to do round-robin selection

            success, output_size, status_code, exception_description = query.execute_workload_item(session, worker_item, timeout=self.query_timeout)
            finish_timestamp = time.time()
            reporter_procotol.report(worker_item.wid, worker_item.queue_timestamp, start_timestamp, finish_timestamp, success, output_size, status_code, exception_description)

    def _select_endpoint(self, query_name):
        self.routing_table_lock.acquire()
        self.logger.info('q={}, i={}, e={}'.format(query_name, self.routing_table_indexer[query_name], self.routing_table[query_name]))
        endpoint = self.routing_table[query_name][self.routing_table_indexer[query_name] % len(self.routing_table[query_name])]
        self.routing_table_indexer[query_name] += 1
        self.routing_table_lock.release()
        return endpoint


class BenchmarkReporterProtocol():
    def __init__(self, worker_id, result_sender):
        self.worker_id = worker_id
        self.result_sender = result_sender
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def report(self, worker_item, queue_timestamp, start_timestemp, finish_timestamp, success, output_size, status_code, exception_description):
        statics = {
            "item": worker_item,
            "queueTimestamp": queue_timestamp,
            "startTimestamp": start_timestemp,
            "finishTimestamp": finish_timestamp,
            "success": success,
            "size": output_size,
            "status": status_code,
            "exception": exception_description
        }
        protocol = BenchmarkProtocol(SendProtocolHeader.report_done, statics)
        self.result_sender.send_pyobj(protocol)
        self.logger.info("report completion: wid=%s, runningTime=%s, totalTime=%s", worker_item, (finish_timestamp-start_timestemp), (finish_timestamp-queue_timestamp))


class BenchmarkSenderProtocol:
    def __init__(self, commander_socket, timeout=3000):
        self.commander_socket = commander_socket
        self.timeout = timeout
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def _send(self, header, param=None):
        request_protocol = BenchmarkProtocol(header, param)
        #self.logger.info(header)
        #self.logger.info(param)
        self.commander_socket.send_pyobj(request_protocol)
        reply_protocol = self.commander_socket.recv_pyobj()
        if reply_protocol.header is not ReceiveProtocolHeader.successful:
            raise Exception("unable to complete the request")
        return reply_protocol.payloads

    def register(self):
        driver_id = self._send(SendProtocolHeader.membership_register)
        self.logger.info("registered driver id={}".format(driver_id))
        return driver_id

    def retrieve_routing_table(self):
        routing_table = self._send(SendProtocolHeader.membership_retrieve_routing_table)
        self.logger.info("routing table size={}".format(len(routing_table)))
        return routing_table

    def ready(self, driver_id):
        self._send(SendProtocolHeader.membership_ready, driver_id)

    def heartbeat(self, driver_id):
        self._send(SendProtocolHeader.membership_heartbeat, driver_id)

    def status(self):
        return self._send(SendProtocolHeader.admin_status)

    def stop(self):
        self._send(SendProtocolHeader.admin_stop)

    def routing_table_upload(self, routing_table):
        self.logger.info("upload routing table")
        return self._send(SendProtocolHeader.routing_table_upload, routing_table)

    def workload_submit(self, workload):
        self.logger.info("submit workload")
        return self._send(SendProtocolHeader.workload_submit, workload)

    def workload_report(self, workload_id):
        self.logger.info("retrieve workload report")
        return self._send(SendProtocolHeader.workload_report, workload_id)

    def workload_statistics(self, workload_id):
        self.logger.info("retrieve workload statistics")
        return self._send(SendProtocolHeader.workload_statistics, workload_id)

    def workload_timeline_completion(self, workload_id):
        self.logger.info("retrieve completion timeline")
        return self._send(SendProtocolHeader.workload_timeline_completion, workload_id)

    def workload_timeline_failure(self, workload_id):
        self.logger.info("retrieve failure timeline")
        return self._send(SendProtocolHeader.workload_timeline_failure, workload_id)

    def workload_status(self, workload_id):
        self.logger.info("check workload status")
        return self._send(SendProtocolHeader.workload_status, workload_id)

    def workload_download(self, workload_id):
        return self._send(SendProtocolHeader.workload_download, workload_id)


class BenchmarkCommander(BenchmarkSenderProtocol):
    def __init__(self, host, port):
        self.host = host
        context = zmq.Context()
        commander_socket = context.socket(zmq.REQ)
        commander_socket.connect("tcp://{}:{}".format(host, port))
        super(BenchmarkCommander, self).__init__(commander_socket)

