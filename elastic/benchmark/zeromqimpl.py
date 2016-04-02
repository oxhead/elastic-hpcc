from enum import Enum
import pickle
import time
import signal
import sys

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

    benchmark_submit = 21
    benchmark_summary = 22
    benchmark_workload = 23

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
        print("@", config_path)
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

    class BenchmarkRecord:
        def __init__(self, workload):
            self.time_start = None
            self.time_stop = None
            self.workload = workload
            self.query_count = 0
            self.num_finished_jobs = 0

        def start(self):
            print("* jobs started")
            self.time_start = time.time()

        def stop(self):
            print("* all jobs completed")
            self.time_stop = time.time()

        def add_workload(self, num_queries):
            self.query_count += num_queries

        def report_completion(self):
            self.num_finished_jobs += 1
            if self.is_completed():
                self.stop()

        def is_completed(self):
            print("@ num_queries={}, num_finished_jobs={}".format(self.query_count, self.num_finished_jobs))
            return self.query_count == self.num_finished_jobs

        def is_started(self):
            return self.time_start is not None

        def get_statistics(self):
            return {
                "num_finished_jobs": self.num_finished_jobs,
                "elapsed_time": self.time_stop - self.time_start,
            }

    class StatusRecord:
        def __init__(self):
            self.status = {}

        def update_status(self, driver_id):
            self.status[driver_id] = time.time()

        def print_status(self):
            for (driver_id, update_time) in self.status.items():
                print("{}: {:.1f}".format(driver_id, update_time))

    class StatisticsCollectorProtocol:
        def __init__(self, controller):
            self.controller = controller

        def _receive(self):
            return self.controller.result_receiver.recv_pyobj()

        def process(self):
            report = self._receive()
            self.controller.benchmark_record.report_completion()
            print(report)

    class CommandReceiverProtocol:
        def __init__(self, controller):
            self.controller = controller
            self.router = {
                SendProtocolHeader.membership_register: self.register,
                SendProtocolHeader.membership_heartbeat: self.heartbeat,
                SendProtocolHeader.membership_ready: self.ready,
                SendProtocolHeader.admin_status: self.status,
                SendProtocolHeader.admin_stop: self.stop,
                SendProtocolHeader.benchmark_summary: self.summary,
                SendProtocolHeader.benchmark_submit: self.submit,
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
                print(e)
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
            self.controller.status_record.update_status(driver_id)

        def status(self, param):
            return self.controller.status_record.status

        def stop(self, param):
            stop_protocol = BenchmarkProtocol(SendProtocolHeader.admin_stop)
            self.controller.manager_publisher.send_pyobj(stop_protocol)

        def summary(self, param):
            return self.controller.benchmark_record.get_statistics()

        def submit(self, workload):
            print("@ workload=", workload)
            # TODO: need to construct a queue?
            self.controller.workload = workload
            self.controller.benchmark_record = BenchmarkController.BenchmarkRecord(self.controller.workload)
            self.controller.ready_drivers = 0
            # need to join?
            workload_protocol = BenchmarkProtocol(SendProtocolHeader.admin_update_workload, self.controller.workload)
            self.controller.manager_publisher.send_pyobj(workload_protocol)
            self.controller.runner_group.spawn(self.controller.dispatcher)
            return "0"

        def get_workload(self, params):
            return self.controller.workload

    def __init__(self, config_path):
        print("Controller initing")
        super(BenchmarkController, self).__init__(config_path)

        self.num_drivers = len(self.config.get_drivers())
        #self.num_queries = self.config.get_config("workload")["num_quries"]
        self.job_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)
        self.result_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)
        self.status_record = BenchmarkController.StatusRecord()

        self.ready_drivers = 0
        self.started_drivers = 0
        self.benchmark_record = None

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
        print('commander...')
        self.commander_socket = self.context.socket(zmq.REP)
        self.commander_socket.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT)))
        self.manager_publisher = self.context.socket(zmq.PUB)
        self.manager_publisher.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT)))

        command_protocol = BenchmarkController.CommandReceiverProtocol(self)

        while True:
            command_protocol.process()
            #gevent.sleep(1)

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
        self.benchmark_record.start()

        # start to dispatch workload - num_queries
        current_workload = self.workload.next()
        while current_workload is not None:
            self.benchmark_record.add_workload(current_workload)
            for i in range(current_workload):
                job_queue.enqueue(str(i))
            print("* waiting for 1 seconds")
            time.sleep(1)
            current_workload = self.workload.next()

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
            print("Running Roxie query")
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


class BenchmarkSenderProtocol():
    def __init__(self, commander_socket):
        self.commander_socket = commander_socket

    def _send(self, header, param=None):
        request_protocol = BenchmarkProtocol(header, param)
        self.commander_socket.send_pyobj(request_protocol)
        reply_protocol = self.commander_socket.recv_pyobj()
        print("@", reply_protocol.header)
        if reply_protocol.header is not ReceiveProtocolHeader.successful:
            raise Exception("unable to complete the request")
        return reply_protocol.payloads

    def register(self):
        driver_id = self._send(SendProtocolHeader.membership_register)
        print("registered driver id={}".format(driver_id))
        return driver_id

    def ready(self, driver_id):
        self._send(SendProtocolHeader.membership_ready, driver_id)

    def heartbeat(self, driver_id):
        self._send(SendProtocolHeader.membership_heartbeat, driver_id)

    def status(self):
        return self._send(SendProtocolHeader.admin_status)

    def stop(self):
        self._send(SendProtocolHeader.admin_stop)

    def summary(self):
        return self._send(SendProtocolHeader.benchmark_summary)

    def submit(self, workload):
        print("@", workload)
        return self._send(SendProtocolHeader.benchmark_submit, workload)

    def get_workload(self):
        return self._send(SendProtocolHeader.benchmark_workload)


class BenchmarkCommander(BenchmarkSenderProtocol):
    def __init__(self, host, port):
        self.host = host
        context = zmq.Context()
        commander_socket = context.socket(zmq.REQ)
        commander_socket.connect("tcp://{}:{}".format(host, port))
        super(BenchmarkCommander, self).__init__(commander_socket)

