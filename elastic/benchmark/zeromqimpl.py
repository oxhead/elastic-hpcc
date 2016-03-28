import time
import signal
import sys

import gevent
import gevent.pool
import gevent.queue
import zmq.green as zmq

from elastic.benchmark import query

class BenchmarkNode:

    def __init__(self, controller_host):
        self.controller_host = controller_host;
        self.context = zmq.Context()
        self.runner_group = gevent.pool.Group()

    def stop(self):
        raise Exception("stop method is not implemented yet")

    def manager(self):
        self.manager_subscriber = self.context.socket(zmq.SUB)
        self.manager_subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.manager_subscriber.connect("tcp://{}:9999".format(self.controller_host))
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

    class ResultQueue:

        def __init__(self, receiver):
            self.receiver = receiver

        def dequeue(self):
            print("Receiving result")
            report = self.receiver.recv_string()
            print("Receiving report: {}".format(report))
            return report

    def __init__(self, controller_host, num_drivers=2, num_quries=1000):
        print("Controller initing")
        super(BenchmarkController, self).__init__(controller_host)
        self.num_drivers = num_drivers
        self.started_drivers = 0
        self.num_quries = num_quries
        self.job_queue_port = 5557
        self.result_queue_port = 5558

    def start(self):
        print("Controller starting")

        self.runner_group.spawn(self.monitor)
        self.runner_group.spawn(self.commander)
        self.runner_group.spawn(self.manager)
        self.runner_group.spawn(self.dispatcher)
        self.runner_group.spawn(self.collector)

        self.runner_group.join()

    def stop(self):
        self.runner_group.kill()

    def commander(self):
        print('commander...')
        self.commander_socket = self.context.socket(zmq.REP)
        self.commander_socket.bind("tcp://*:10000")
        self.manager_publisher = self.context.socket(zmq.PUB)
        self.manager_publisher.bind("tcp://*:9999")
        while True:
            print("Receiving...")
            cmd = self.commander_socket.recv_string()
            self.commander_socket.send_string("")
            print("[Commander] cmd={}".format(cmd))
            if cmd == "register":
                self.started_drivers = self.started_drivers + 1
                print("# of drivers=", self.started_drivers)
            elif cmd == "stop":
                self.manager_publisher.send_string(cmd)
            gevent.sleep(1)

    def dispatcher(self):
        print('dispatcher...')
        self.job_sender = self.context.socket(zmq.PUSH)
        self.job_sender.bind("tcp://*:{}".format(self.job_queue_port))
        job_queue = BenchmarkController.JobQueue(self.job_sender)

        while self.started_drivers < self.num_drivers:
            gevent.sleep(1)

        for i in range(self.num_quries):
            job_queue.enqueue(str(i))
            # time.sleep(1)

    def collector(self):
        print('collector...')
        self.result_receiver = self.context.socket(zmq.PULL)
        self.result_receiver.bind("tcp://*:{}".format(self.result_queue_port))
        result_queue = BenchmarkController.ResultQueue(self.result_receiver)
        num_collected = 0
        while num_collected < 100:
            worker_id = result_queue.dequeue()
            print("Received from {}".format(worker_id))


class BenchmarkDriver(BenchmarkNode):

    def __init__(self, controller_host, num_workers=8):
        super(BenchmarkDriver, self).__init__(controller_host)
        print("Driver initing")
        self.num_workers = num_workers
        self.worker_pool = gevent.pool.Pool(self.num_workers)
        self.worker_queue = gevent.queue.Queue()

    def start(self):
        print("Driver starting at {}".format(self.controller_host))

        for i in range(self.num_workers):
            self.worker_pool.spawn(self.worker, i)

        self.runner_group.spawn(self.retriever)
        self.runner_group.spawn(self.manager)
        self.runner_group.spawn(self.monitor)

        self.runner_group.join()

    def stop(self):
        self.worker_pool.kill()
        self.runner_group.kill()

    def _register(self):
        BenchmarkCommander(self.controller_host).register()

    def retriever(self):
        self.receiver = self.context.socket(zmq.PULL)
        self.sender = self.context.socket(zmq.PUSH)
        self.receiver.connect("tcp://{}:5557".format(self.controller_host))
        self.sender.connect("tcp://{}:5558".format(self.controller_host))

        self._register()

        while True:
            if self.worker_queue.qsize() > self.worker_pool.size * 2:
                print("@ too much job, yield to others")
                gevent.sleep(1)
            print("waiting for message")
            job_id = self.receiver.recv_string()
            print("Receved job: {}".format(job_id))
            self.worker_queue.put(job_id)

    def worker(self, worker_id):
        while True:
            worker_item = self.worker_queue.get()
            print("Running oxie query")
            query.run_query()
            self.sender.send_string("{} finished {}".format(worker_id, worker_item))


class BenchmarkCommander:
    def __init__(self, host):
        self.host = host
        self.context = zmq.Context()
        self.commander_socket = self.context.socket(zmq.REQ)
        self.commander_socket.connect("tcp://{}:10000".format(host))

    def msg(self, message):
        self.commander_socket.send_string(message)
        self.commander_socket.recv_string()

    def register(self):
        self.msg("register")

    def stop(self):
        self.commander_socket.send_string("stop")
        self.commander_socket.recv_string()
