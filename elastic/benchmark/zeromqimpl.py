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
from elastic.benchmark import base


class BenchmarkConfig(base.BenchmarkConfig):

    CONTROLLER_HOST = "controller.host"
    CONTROLLER_COMMANDER_PORT = "controller.commander.port"
    CONTROLLER_JOB_QUEUE_PORT = "controller.job_queue.port"
    CONTROLLER_REPORT_QUEUE_PORT = "controller.report_queue.port"
    CONTROLLER_MANAGER_PORT = "controller.manager.port"

    DRIVER_NUM_WORKER = "driver.num_workers"

    WORKLOAD_NUM_QUERIES = "workload.num_queries"
    WORKLOAD_APPLICATIONS = "workload.applications"

    def parse_file(config_path):
        with open(config_path, 'r') as f:
            return BenchmarkConfig(yaml.load(f))

    def __init__(self, config):
        self.config = config

    def lookup_config(self, key, default_value=None):
        key_list = key.split(".")
        current_config = self.config
        for k in key_list:
            current_config = current_config[k]
        return current_config

class BenchmarkNode:

    def __init__(self, config_path):
        print("@", config_path)
        self.config = BenchmarkConfig.parse_file(config_path)
        self.context = zmq.Context()
        self.runner_group = gevent.pool.Group()

    def stop(self):
        raise Exception("stop method is not implemented yet")

    def manager(self):
        self.manager_subscriber = self.context.socket(zmq.SUB)
        self.manager_subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.manager_subscriber.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT)))
        while True:
            cmd = self.manager_subscriber.recv_string()
            print("Received command: {}".format(cmd))
            if cmd == "stop":
                self.stop();

    def monitor(self):
        while True:
            print("monitoring...")
            for runner in self.runner_group.greenlets:
                if not bool(runner):
                    raise Exception("Abnomal greenlet found")
            gevent.sleep(5)


class BenchmarkController(BenchmarkNode):

    class JobQueue:

        def __init__(self, sender):
            self.sender = sender

        def enqueue(self, job_id):
            print("Sending job: {}".format(job_id))
            self.sender.send_string(job_id)

    class BenchmarkRecord:
        def __init__(self, num_queries):
            self.time_start = None
            self.time_stop = None
            self.num_finished_jobs = 0
            self.num_queries = num_queries

        def start(self):
            self.time_start = time.time()

        def stop(self):
            self.time_stop = time.time()

        def report_completion(self):
            self.num_finished_jobs += 1
            print("# finished:", self.num_finished_jobs)
            if self.is_completed():
                self.stop()

        def is_completed(self):
            return self.num_queries == self.num_finished_jobs

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
                "register": self.register,
                "heartbeat": self.heartbeat,
                "status": self.status,
                "stop": self.stop,
                "summary": self.summary,
                "submit": self.submit,
            }

        def _receive(self):
            request_data = self.controller.commander_socket.recv_multipart()
            #print(request_data)
            cmd = request_data[0].decode()
            #print("@", cmd)
            params = request_data[1:]
            return (cmd, params)

        def process(self):
            (cmd, params) = self._receive()
            print("cmd=", cmd)
            func = self.router[cmd]
            result = func(params)
            if result is None:
                self.controller.commander_socket.send_string("")
            else:
                self.controller.commander_socket.send_pyobj(result)

        def register(self, params):
            self.controller.started_drivers = self.controller.started_drivers + 1
            return str(self.controller.started_drivers)

        def heartbeat(self, params):
            driver = params[0].decode()
            #print("received hearbeat from ", driver)
            self.controller.status_record.update_status(driver)

        def status(self, params):
            return self.controller.status_record.status

        def stop(self, params):
            self.controller.manager_publisher.send_string("stop")

        def summary(self, params):
            return self.controller.benchmark_record.get_statistics()

        def submit(self, params):
            workload = pickle.loads(params[0])
            print("@ workload=", workload)
            # TODO: need to construct a queue?
            self.controller.benchmark_record = BenchmarkController.BenchmarkRecord(workload['num_queries'])
            # need to join?
            self.controller.runner_group.spawn(self.controller.dispatcher)
            return "0"

    def __init__(self, config_path):
        print("Controller initing")
        super(BenchmarkController, self).__init__(config_path)

        self.num_drivers = len(self.config.get_drivers())
        #self.num_queries = self.config.get_config("workload")["num_quries"]
        self.job_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_JOB_QUEUE_PORT)
        self.result_queue_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORT_QUEUE_PORT)
        self.status_record = BenchmarkController.StatusRecord()

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
        while self.started_drivers < self.num_drivers:
            gevent.sleep(1)

        # start the counter
        self.benchmark_record.start()

        # start to dispatch workload
        for i in range(self.benchmark_record.num_queries):
            job_queue.enqueue(str(i))
            # time.sleep(1)

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
            print("Receved job: {}".format(job_id))
            self.worker_queue.put(job_id)

    def worker(self, worker_id):
        query_factory = query.QueryFactory(self.config.lookup_config(BenchmarkConfig.WORKLOAD_APPLICATIONS))
        reporter_procotol = BenchmarkReporterProtocol(worker_id, self.sender)
        while True:
            worker_item = self.worker_queue.get()
            print("Running Roxie query")
            start_time = time.time()
            query_factory.next()()
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
        self.result_sender.send_pyobj(statics)


class BenchmarkSenderProtocol():
    def __init__(self, commander_socket):
        self.commander_socket = commander_socket

    def _send(self, cmd, *param):
        self.commander_socket.send_multipart([cmd.encode()] + list(param))
        return self.commander_socket.recv_pyobj()

    def _send_no_reply(self, cmd, *param):
        self.commander_socket.send_multipart([cmd.encode()] + list(param))
        self.commander_socket.recv_string()

    def register(self):
        driver_id = self._send("register")
        print("registered driver id={}".format(driver_id))
        return driver_id

    def heartbeat(self, driver_id):
        self._send_no_reply("heartbeat", str(driver_id).encode())

    def status(self):
        status_record = self._send("status")
        return status_record

    def start(self):
        self._send_no_reply("start")

    def stop(self):
        self._send_no_reply("stop")

    def summary(self):
        report = self._send("summary")
        return report

    def submit(self, workload):
        print(pickle.dumps(workload))
        benchmark_id = self._send("submit", pickle.dumps(workload))
        return benchmark_id


class BenchmarkCommander(BenchmarkSenderProtocol):
    def __init__(self, host, port):
        self.host = host
        context = zmq.Context()
        commander_socket = context.socket(zmq.REQ)
        commander_socket.connect("tcp://{}:{}".format(host, port))
        super(BenchmarkCommander, self).__init__(commander_socket)

