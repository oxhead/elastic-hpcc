from enum import Enum
import itertools
import logging

import gevent

from elastic.benchmark import query
from elastic.benchmark import config


class BenchmarkConfig(config.BaseConfig):

    CONTROLLER_HOST = "controller.host"
    CONTROLLER_CLIENT_PORT = "controller.client.port"
    CONTROLLER_DRIVER_PORT = "controller.driver.port"
    CONTROLLER_REPORTER_PORT = "controller.reporter.port"
    CONTROLLER_MANAGER_PORT = "controller.manager.port"

    DRIVER_NUM_WORKER = "driver.num_workers"
    DRIVER_QUERY_TIMEOUT = "driver.query_timeout"
    DRIVER_MANUAL_ROUTING = "driver.manual_routing"

    WORKLOAD_NUM_QUERIES = "workload.num_queries"
    WORKLOAD_APPLICATIONS = "workload.applications"


class BenchmarkNode:

    def __init__(self, config_path):
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
        self.logger.info("load benchmark configuration from {}".format(config_path))
        self.config = BenchmarkConfig.parse_file(config_path)
        self.context = zmq.Context()
        self.runner_group = gevent.pool.Group()  # group of threads need to be monitored

    def stop(self):
        raise Exception("stop method is not implemented yet")

    def echo(self):
        raise Exception("echo is not implemented yet")

    def worker_monitor(self):
        while True:
            self.logger.info("monitoring...")
            for runner in self.runner_group.greenlets:
                if not bool(runner):
                    raise Exception("Abnormal greenlet found")
            gevent.sleep(5)


class RequestType(Enum):
    queue = 0
    run = 1
    empty = 2
    stop = 3


class Request:

    counter = itertools.count()

    @staticmethod
    def new_empty():
        return Request(RequestType.empty, -1, None)

    @staticmethod
    def new_stop():
        return Request(RequestType.stop, -1, None)

    @staticmethod
    def new(workload):
        return Request(RequestType.queue, next(Request.counter), workload)

    def __init__(self, xtype, xid, workload):
        self.xtype = xtype
        self.xid = xid
        self.workload = workload


class RequestWorker:

    counter = itertools.count()

    def __init__(self, node_bucket):
        next(RequestWorker.counter)
        self.xid = RequestWorker.counter
        self.node_bucket = node_bucket
        self.count = 0

    def next_request(self):
        return self.node_bucket.get_request()

    def runner(self):
        session = query.new_session()
        while True:
            request = self.next_request()
            if request.xtype == RequestType.empty:
                gevent.sleep(1)
            elif request.xtype == RequestType.stop:
                break
            elif request.xtype == RequestType.run:
                self.count += 1
                query.execute_workload_item(session, request.workload)
                gevent.sleep(0)  # no need to sleep because the node_bucket will do blocking call?


class SimpleBucket:
    '''
    A wrap class for the gevent Queue
    '''
    def __init__(self, xid):
        self.xid = xid
        self.queue = gevent.queue.Queue()
        self.count_put = 0
        self.count_get = 0
        self.completed = False

    def complete(self):
        self.completed = True

    def is_completed(self):
        return self.completed

    def empty(self):
        return self.queue.empty()

    def qsize(self):
        return self.queue.qsize()

    def put(self, request):
        self.queue.put(request)
        self.count_put += 1

    def get(self, timeout=1):
        obj = self.queue.get()
        self.count_get -= 1
        return obj

    def get_wait(self, timeout=1):
        try:
            obj = self.queue.get(timeout=timeout)
            self.count_get -= 1
            return obj
        except:
            pass
        return None

    def get_nowait(self):
        try:
            obj = self.queue.get_nowait()
            self.count_get -= 1
            return obj
        except:
            pass
        return None

    def peak(self):
        try:
            return self.queue.peek_nowait()
        except:
            pass
        return None


class NodeBucket(SimpleBucket):
    def __init__(self, xid, endpoint):
        super().__init__(xid)
        self.endpoint = endpoint
        self.taken_set = set()  # requests are taken in other sets
        self.dispatch_completed = False
        self.request_query_mapping = {}

    def complete_dispatch(self):
        self.dispatch_completed = True

    def put_request(self, request, query_bucket):
        self.put(request)
        self.request_query_mapping[request] = query_bucket

    def get_request(self):
        '''
        None blocking call??  The client should reduce the calling period??
        :return:
        '''
        # when the queue it not empty
        # even the requests are all dispatched to this node bucket
        while not self.empty():
            request = self.get_nowait()
            # This should not happen?
            if request is None:
                return Request.new_empty()
            else:
                # already taken by other nodes
                # probably don't need to eliminate this request in the taken_set
                # not a big issue for memory capacity?
                if request in self.taken_set:
                    continue
                else:
                    # notify the corresponding query bucket
                    # synchronized call, efficient? iterate over all other node bucket to the same query bucket
                    # request.query_bucket.notify_taken(request, self)
                    self.request_query_mapping[request].notify_taken(request, self)
                    request.xtype = RequestType.run
                    return request

        if not self.dispatch_completed:
            gevent.sleep(1)  # good timeout?
            return self.get_request()
        else:
            return Request.new_stop()

    def notify_taken(self, request):
        self.taken_set.add(request)


class QueryBucket:
    def __init__(self, name):
        self.name = name
        self.node_buckets = []

    def add_node_bucket(self, node_bucket):
        self.node_buckets.append(node_bucket)

    def put_request(self, request):
        for node_bucket in self.node_buckets:
            # should be in order for each queue??
            node_bucket.put_request(request, self)

    def complete_dispatch(self):
        for node_bucket in self.node_buckets:
            node_bucket.complete_dispatch()

    def notify_taken(self, request, node_bucket):
        for other_node_bucket in self.node_buckets:
            # is this safe comparison?
            #if other_node_bucket != node_bucket:
            #    other_node_bucket.notify_taken(request)
            # should be safe even add the record to the node bucket which takes the request
            other_node_bucket.notify_taken(request)