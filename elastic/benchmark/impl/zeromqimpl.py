from enum import Enum
import pickle
import time
import signal
import sys
import logging
import json
import random

import gevent
import gevent.pool
import gevent.queue
from gevent.pool import Group
from gevent.lock import BoundedSemaphore
import yaml
import zmq.green as zmq

from elastic.benchmark import query
from elastic.benchmark import config
from elastic.benchmark import workload


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
    pass





class Request:
    def __init__(self, id, workload):
        self.id = id
        self.workload = workload

class RequestWorker:
    def __init__(self, id, node_handler):
        self.id = id
        self.node_handler = node_handler
        self.count = 0
    def runner(self):
        session = query.new_session()
        while not (self.node_handler.is_completed() and self.node_handler.empty()):
            try:
                req = self.node_handler.get_wait(1)
                if req is not None:
                    #print('worker {} is processing request {}'.format(self.id, req.order))
                    self.count += 1
                    t = random.random()
                    #print('running for {} seconds'.format(t))
                    #print(self.id, req.workload.query_name, req.workload.endpoint)
                    query.execute_workload_item(session, req.workload)
                    #gevent.sleep(t)
                    #gevent.sleep(0)
                else:
                    #print('worker {} yield because qsize={}'.format(self.id, self.node_handler.qsize()))
                    gevent.sleep(0)
            except Exception as e:
                import traceback
                traceback.print_exc()
            #gevent.sleep(0)



class SimpleBucket:
    def __init__(self, id):
        self.id = id
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


class RequestGenerator:
    def __init__(self, num_requests, request_dispatchers):
        self.num_requests = num_requests
        self.request_dispatchers = request_dispatchers


    def runner(self):
        endpoints = [
            "http://10.25.2.131:9876",
            "http://10.25.2.132:9876",
            "http://10.25.2.133:9876",
            "http://10.25.2.134:9876",
            "http://10.25.2.136:9876",
            "http://10.25.2.138:9876",
            "http://10.25.2.139:9876",
            "http://10.25.2.140:9876",
        ]

        key_list = open('/home/chsu6/elastic-hpcc/benchmark/dataset/firstname_list_1085.txt', 'r').readlines()

        count = 0
        for i in range(self.num_requests):
            app_id = random.randint(1, 1024)
            query_name = "sequential_search_firstname_{}".format(app_id)
            query_key = 'firstname'
            key_firstname = key_list[random.randint(0, len(key_list)-1)].strip()
            endpoint = None
            if app_id <= 256:
                endpoint = random.choice(endpoints[0:2])
            elif app_id <= 512:
                endpoint = random.choice(endpoints[2:4])
            elif app_id <= 768:
                endpoint = random.choice(endpoints[4:6])
            elif app_id <= 1024:
                endpoint = random.choice(endpoints[6:8])

            workload_item = workload.WorkloadItem(str(count), query_name, endpoint, query_key, key_firstname)
            req = Request(i, workload_item)

            #request_dispatcher = request_dispatchers[random.randint(0, len(request_dispatchers) - 1)]
            request_dispatcher = request_dispatchers[app_id-1]
            request_dispatcher.put(req)

        for request_dispatcher in self.request_dispatchers:
            request_dispatcher.complete()
        print('Completed request generator')


class RequestDispatcherBucket(SimpleBucket):
    def __init__(self, id):
        super(RequestDispatcherBucket, self).__init__(id)


class NodeHandlerBucket(SimpleBucket):
    def __init__(self, id):
        super(NodeHandlerBucket, self).__init__(id)
        self.queue = gevent.queue.Queue()
        self.task_completed = False
        self.dispatcher_buckets = []
        self.dispatcher_completion_records = {}
        self.bucket_count = 0
    def add_dispatcher_bucket(self, dispatcher_bucket):
        self.dispatcher_buckets.append(dispatcher_bucket)
    def runner2(self):
        while not self.task_completed:
            try:
                #print('Handler {} is processing'.format(self.id))
                if len(self.dispatcher_buckets) == 0:
                    break
                earliest_dispatcher = self.dispatcher_buckets[0]
                earliest_request = earliest_dispatcher.peak()
                for dispatcher in self.dispatcher_buckets[1:]:
                    request = dispatcher.peak()
                    if earliest_request is None:
                        earliest_request = request
                    if (earliest_request is not None) and (request is not None):
                        # small than should be fine??
                        earliest_dispatcher = earliest_dispatcher if earliest_request.order < request.order else dispatcher
                        earliest_request = earliest_request if earliest_request.order < request.order else request
                if earliest_request is not None:
                    picked_request = earliest_dispatcher.get()
                    assert picked_request == earliest_request
                    self.queue.put(picked_request)
                gevent.sleep(0)
            except Exception as e:
                import traceback
                traceback.print_exec()
            #gevent.sleep(1)

    def runner(self):
        if len(self.dispatcher_buckets) == 0:
            return
        try:
            # check all dispatcher completed?
            while True:
                count = 0
                request_dispatcher = self.dispatcher_buckets[self.bucket_count % len(self.dispatcher_buckets)]
                while request_dispatcher.empty() and count < len(self.dispatcher_buckets):
                    count += 1
                    self.bucket_count += 1
                    request_dispatcher = self.dispatcher_buckets[self.bucket_count % len(self.dispatcher_buckets)]

                req = request_dispatcher.get_nowait()
                if req is not None:
                    self.put(req)
                else:
                    if all([dispatcher.is_completed() for dispatcher in self.dispatcher_buckets]):
                        print('handler {} has no more request to fetch'.format(self.id))
                        break
                self.bucket_count += 1
                gevent.sleep(0)
            self.complete()
            print('handler {} completed'.format(self.id))
        except:
            import traceback
            traceback.print_exc()


def completion_checker(request_dispatchers, node_handlers):
    for dispatcher in request_dispatchers:
        while not dispatcher.empty():
            #print(dispatcher.id, dispatcher.queue.qsize())
            #gevent.sleep(1)
            gevent.sleep(0)
    print('all dispatcher completes')
    for node_handler in node_handlers:
        while not node_handler.empty():
            print('handler {}: {}'.format(node_handler.id, node_handler.qsize()))
            gevent.sleep(1)
    for node_handler in node_handlers:
        node_handler.task_completed = True

if __name__ == '__main__':
    num_requests = 30000
    num_request_dispatchers = 1024
    num_node_handlers = 8
    num_workers_per_node = 16
    request_dispatchers = [RequestDispatcherBucket(id) for id in range(num_request_dispatchers)]
    node_handlers = [NodeHandlerBucket(id) for id in range(num_node_handlers)]
    current_index = 0

    for i in range(num_node_handlers):
        start_index = 0
        if i < 2:
            start_index = 0
        elif i < 4:
            start_index = 256
        elif i < 6:
            start_index = 512
        elif i < 8:
            start_index = 768
        for dispatcher_index in range(start_index, start_index+256):
            node_handlers[i].add_dispatcher_bucket(request_dispatchers[dispatcher_index])
        #num_dispatchers_per_node = int(num_request_dispatchers / num_node_handlers * 2)
        #for index in sorted(random.sample(range(num_request_dispatchers), 256)):
        #for _ in range(num_dispatchers_per_node):
        #    current_index += 1
        #    node_handlers[i].add_dispatcher_bucket(request_dispatchers[current_index%num_request_dispatchers])
        print('handler {} has {} dispatchers'.format(node_handlers[i].id, len(node_handlers[i].dispatcher_buckets)))
    print('-----------')

    group = Group()
    # Request Generator
    request_generator = RequestGenerator(num_requests, request_dispatchers)
    group.add(gevent.spawn(request_generator.runner))

    # Request Worker
    worker_count = 0
    workers = []
    for node_handler in node_handlers:
        for i in range(num_workers_per_node):
            worker = RequestWorker(worker_count, node_handler)
            worker_count += 1
            group.add(gevent.spawn(worker.runner))
            workers.append(worker)
    print('Total workers:', len(workers))

    # Node Handler
    for node_handler in node_handlers:
        group.add(gevent.spawn(node_handler.runner))
    group.add(gevent.spawn(completion_checker, request_dispatchers, node_handlers))
    group.join()
    #worker_group.join()

    print('=========')
    print('num of dispatch put:', sum([dispatcher.count_put for dispatcher in request_dispatchers]))
    print('num of dispatch get:', sum([dispatcher.count_get for dispatcher in request_dispatchers]))
    print('num of handler put:', sum([handler.count_put for handler in node_handlers]))
    print('num of handler get:', sum([handler.count_get for handler in node_handlers]))
    print('num of workers:', worker_count)
    print('completions:', sum([worker.count for worker in workers]))
    print('done')



