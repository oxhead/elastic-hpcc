from enum import Enum
import pickle
import time
import signal
import sys
import logging

import gevent
import gevent.pool
import gevent.queue
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

    WORKLOAD_NUM_QUERIES = "workload.num_queries"
    WORKLOAD_APPLICATIONS = "workload.applications"


class SendProtocolHeader(Enum):
    admin_start = 0
    admin_stop = 1
    admin_status = 2
    admin_update_workload = 3

    membership_heartbeat = 11
    membership_register = 12
    membership_ready = 13

    workload_submit = 21
    workload_status = 22
    workload_report = 23
    workload_statistics = 24
    workload_download = 25

    report_done = 31


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

    def manager(self):
        self.manager_subscriber = self.context.socket(zmq.SUB)
        self.manager_subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.manager_subscriber.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT)))
        while True:
            cmd_protocol = self.manager_subscriber.recv_pyobj()
            print("manager:", cmd_protocol.header)
            if cmd_protocol.header is SendProtocolHeader.admin_stop:
                self.stop()
            elif cmd_protocol.header is SendProtocolHeader.admin_update_workload:
                self.update_workload(cmd_protocol.payloads)

    def monitor(self):
        while True:
            print("monitoring...")
            for runner in self.runner_group.greenlets:
                if not bool(runner):
                    raise Exception("Abnormal greenlet found")
            gevent.sleep(5)


class BenchmarkController(BenchmarkNode):

    class JobQueue:

        def __init__(self, sender):
            self.sender = sender

        def enqueue(self, job_id):
            print("Sending job: {}".format(job_id))
            self.sender.send_string(job_id)

    class WorkloadRecord:
        def __init__(self, workload_id, workload):
            self.time_start = None
            self.time_stop = None
            self.workload_id = workload_id
            self.workload = workload
            self.query_count = 0
            self.num_finished_jobs = 0
            self.ready_drivers = 0
            self.dispatch_completed = False
            self.statistics = {}

        def start(self):
            print("* jobs started")
            self.time_start = time.time()

        def stop(self):
            print("* all jobs completed")
            self.time_stop = time.time()

        def get_workload(self):
            return self.workload

        def add_workload(self, num_queries):
            self.query_count += num_queries

        def completed_dispatch(self):
            print("@ job dispatch completed")
            self.dispatch_completed = True

        def report_completion(self, report):
            self.num_finished_jobs += 1
            self.statistics[report['item']] = report['elapsedTime']
            if self.is_completed():
                self.stop()

        def is_completed(self):
            print("# dispatch completed:", self.dispatch_completed)
            print("@ num_queries={}, num_finished_jobs={}".format(self.query_count, self.num_finished_jobs))
            return self.dispatch_completed and (self.query_count == self.num_finished_jobs)

        def is_started(self):
            return self.time_start is not None

        def get_report(self):
            return {
                "num_finished_jobs": self.num_finished_jobs,
                "elapsed_time": self.time_stop - self.time_start if self.is_completed() else time.time() - self.time_start
            }

        def get_statistics(self):
            return self.statistics

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
            self.controller = controller
            self.router = {
                SendProtocolHeader.membership_register: self.register,
                SendProtocolHeader.membership_heartbeat: self.heartbeat,
                SendProtocolHeader.membership_ready: self.ready,
                SendProtocolHeader.admin_status: self.status,
                SendProtocolHeader.admin_stop: self.stop,
                SendProtocolHeader.workload_submit: self.workload_submit,
                SendProtocolHeader.workload_report: self.workload_report,
                SendProtocolHeader.workload_statistics: self.workload_statistics,
                SendProtocolHeader.workload_status: self.workload_status,
            }

        def process(self):
            cmd_protocol = self.controller.commander_socket.recv_pyobj()
            print("cmd=", cmd_protocol.header)
            print("payload=", cmd_protocol.payloads)
            reply_protocol = None
            try:
                result = self.router[cmd_protocol.header](cmd_protocol.payloads)
                print("result=", result)
                reply_protocol = BenchmarkProtocol(ReceiveProtocolHeader.successful, result)
            except Exception as e:
                import traceback
                traceback.print_exc()
                reply_protocol = BenchmarkProtocol(ReceiveProtocolHeader.failed)
            self.controller.commander_socket.send_pyobj(reply_protocol)

        def register(self, param):
            self.controller.started_drivers += 1
            return str(self.controller.started_drivers)

        def ready(self, param):
            print("@ report ready")
            self.controller.ready_drivers += 1

        def heartbeat(self, driver_id):
            # print("received heartbeat from ", driver)
            self.controller.node_status_record.update_status(driver_id)

        def status(self, workload_id):
            return self.controller.node_status_record.status

        def stop(self, param):
            stop_protocol = BenchmarkProtocol(SendProtocolHeader.admin_stop)
            self.controller.manager_publisher.send_pyobj(stop_protocol)

        def workload_submit(self, workload):
            print("@ workload=", workload)
            if (self.controller.current_workload_record is not None) and (not self.controller.current_workload_record.is_completed()):
                raise Exception("need to wait the current workload to complete")
            new_workload_id = len(self.controller.workload_db)
            self.controller.current_workload_record = BenchmarkController.WorkloadRecord(new_workload_id, workload)
            self.controller.workload_db[str(new_workload_id)] = self.controller.current_workload_record
            workload_protocol = BenchmarkProtocol(SendProtocolHeader.admin_update_workload, self.controller.current_workload_record.get_workload())
            self.controller.manager_publisher.send_pyobj(workload_protocol)
            self.controller.runner_group.spawn(self.controller.dispatcher)
            return str(new_workload_id)

        def workload_report(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_report()

        def workload_statistics(self, workload_id):
            return self.controller.workload_db[str(workload_id)].get_statistics()

        def workload_status(self, workload_id):
            return self.controller.workload_db[str(workload_id)].is_completed()

        def workload_download(self, workload_id):
            return self.controller.workload_db[str(workload_id).get_workload()]

    def __init__(self, config_path):
        print("Controller initing")
        super(BenchmarkController, self).__init__(config_path)

        self.num_drivers = len(self.config.get_drivers())
        # self.num_queries = self.config.get_config("workload")["num_quries"]
        self.job_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)
        self.result_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)
        self.node_status_record = BenchmarkController.NodeStatusRecord()

        self.ready_drivers = 0
        self.started_drivers = 0

        self.workload_db = {}
        self.current_workload_record = None

        self.logger = logging.getLogger(__name__)

    def start(self):
        print("Controller starting")

        self.runner_group.spawn(self.monitor)
        self.runner_group.spawn(self.commander)
        self.runner_group.spawn(self.manager)
        self.runner_group.spawn(self.collector)

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

        while True:
            command_protocol.process()

    def dispatcher(self):
        print('dispatcher...')
        job_sender = self.context.socket(zmq.PUSH)
        job_sender.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)))
        job_queue = BenchmarkController.JobQueue(job_sender)

        # wait for all drivers to start
        while self.ready_drivers < self.num_drivers:
            print("# drivers: ready={}, total={}".format(self.ready_drivers, self.num_drivers))
            gevent.sleep(1)

        # start the counter
        self.current_workload_record.start()

        # start to dispatch workload - num_queries
        current_workload = self.current_workload_record.get_workload()

        current_cycle = 0
        num_queries = current_workload.next()
        while num_queries is not None:
            current_cycle += 1
            self.current_workload_record.add_workload(num_queries)
            # TODO: needs to fix here for correct/better job id
            for i in range(num_queries):
                job_queue.enqueue("{}-{}".format(current_cycle, i+1))
            num_queries = current_workload.next()
            if num_queries is None:
                self.current_workload_record.completed_dispatch()
            print("* waiting for 1 seconds")
            time.sleep(1)

        job_sender.close()

    def collector(self):
        print('collector...')
        self.result_receiver = self.context.socket(zmq.PULL)
        self.result_receiver.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)))

        reporter_protocol = BenchmarkController.StatisticsCollectorProtocol(self)

        while True:
            reporter_protocol.process()


class BenchmarkDriver(BenchmarkNode):

    def __init__(self, config_path):
        super(BenchmarkDriver, self).__init__(config_path)
        print("Driver initing")
        self.num_workers = self.config.lookup_config(BenchmarkConfig.DRIVER_NUM_WORKER)
        self.worker_pool = gevent.pool.Pool(self.num_workers)
        self.worker_queue = gevent.queue.Queue()
        self.workload = None

    def start(self):
        print("Driver starting")

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

        self.runner_group.join()

    def stop(self):
        self.worker_pool.kill()
        self.runner_group.kill()

    def update_workload(self, workload):
        self.workload = workload
        BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).ready(self.driver_id)

    def _register(self):
        self.driver_id = BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)).register()

    def heartbeater(self):
        self.heartbeater_socket = self.context.socket(zmq.REQ)
        self.heartbeater_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)))
        heartbeat_protocol = BenchmarkSenderProtocol(self.heartbeater_socket)
        while True:
            heartbeat_protocol.heartbeat(self.driver_id)
            gevent.sleep(5)

    def retriever(self):
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)))

        while True:
            if self.worker_queue.qsize() > self.worker_pool.size * 2:
                print("@ too much job, yield to others")
                gevent.sleep(1)
            print("waiting for message")
            job_id = self.receiver.recv_string()
            print("Received job: {}".format(job_id))
            self.worker_queue.put(job_id)

    def worker(self, worker_id):
        session = query.new_session()
        reporter_procotol = BenchmarkReporterProtocol(worker_id, self.sender)
        while True:
            worker_item = self.worker_queue.get()
            self.logger.info("worker {} is processing roxie query {}".format(worker_id, worker_item))
            app = self.workload.application_selection.select()
            start_time = time.time()
            (query_name, endpoint, query_key, key) = app.next_query()
            query.run_query(session, *app.next_query())
            elapsed_time = time.time() - start_time
            reporter_procotol.report(worker_item, elapsed_time)


class BenchmarkReporterProtocol():
    def __init__(self, worker_id, result_sender):
        self.worker_id = worker_id
        self.result_sender = result_sender

    def report(self, worker_item, elaspsed_time):
        statics = {
            "item": worker_item,
            "elapsedTime": elaspsed_time
        }
        protocol = BenchmarkProtocol(SendProtocolHeader.report_done, statics)
        self.result_sender.send_pyobj(protocol)


class BenchmarkSenderProtocol:
    def __init__(self, commander_socket, timeout=3000):
        self.commander_socket = commander_socket
        self.timeout = timeout
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def _send(self, header, param=None):
        request_protocol = BenchmarkProtocol(header, param)
        self.logger.debug(header)
        self.logger.debug(param)
        self.commander_socket.send_pyobj(request_protocol)
        reply_protocol = self.commander_socket.recv_pyobj()
        if reply_protocol.header is not ReceiveProtocolHeader.successful:
            raise Exception("unable to complete the request")
        return reply_protocol.payloads

    def register(self):
        driver_id = self._send(SendProtocolHeader.membership_register)
        self.logger.info("registered driver id={}".format(driver_id))
        return driver_id

    def ready(self, driver_id):
        self._send(SendProtocolHeader.membership_ready, driver_id)

    def heartbeat(self, driver_id):
        self._send(SendProtocolHeader.membership_heartbeat, driver_id)

    def status(self):
        return self._send(SendProtocolHeader.admin_status)

    def stop(self):
        self._send(SendProtocolHeader.admin_stop)

    def workload_submit(self, workload):
        self.logger.info("submit workload")
        return self._send(SendProtocolHeader.workload_submit, workload)

    def workload_report(self, workload_id):
        self.logger.info("retrieve workload report")
        return self._send(SendProtocolHeader.workload_report, workload_id)

    def workload_statistics(self, workload_id):
        self.logger.info("retrieve workload statistics")
        return self._send(SendProtocolHeader.workload_statistics, workload_id)

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

