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
from gevent import monkey
from gevent.pool import Group
from gevent.lock import BoundedSemaphore
import yaml
import zmq.green as zmq

from elastic.benchmark import query
from elastic.benchmark import config
from elastic.benchmark import workload
from elastic.benchmark.impl.base import *
from elastic.benchmark.impl.protocol import *
from elastic.util import helper

monkey.patch_all()


class BenchmarkConfig(config.BaseConfig):

    CONTROLLER_HOST = "controller.host"
    CONTROLLER_CLIENT_PORT = "controller.client.port"
    CONTROLLER_DRIVER_PORT = "controller.driver.port"
    CONTROLLER_REPORTER_PORT = "controller.reporter.port"
    CONTROLLER_MANAGER_PORT = "controller.manager.port"

    DRIVER_NUM_PROCESSORS = "driver.num_processors"
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
            # self.logger.info("monitoring...")
            for runner in self.runner_group.greenlets:
                if not bool(runner):
                    raise Exception("Abnormal greenlet found")
            gevent.sleep(5)


class BenchmarkController(BenchmarkNode):

    class ClusterManager:
        def __init__(self, controller, num_drivers, port):
            self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
            self.controller = controller
            self.num_drivers = num_drivers
            self.port = port
            self.ready_drivers = 0
            self.driver_status = {}

        def start(self):
            self.manager_publisher = self.controller.context.socket(zmq.PUB)
            self.manager_publisher.bind("tcp://*:{}".format(self.port))

        def heartbeat_from_driver(self, driver_id, status):
            # self.logger.info("Driver {} sends heartbeat {}".format(driver_id, status))
            self.driver_status[driver_id] = (status, time.time())

        def get_cluster_status(self):
            return self.driver_status

        def sync_from_driver(self, driver_id):
            # maybe need to record the which driver in the future?
            self.logger.info("Driver {} is ready".format(driver_id))
            self.ready_drivers += 1

        def reset_ready_drivers(self):
            self.ready_drivers = 0

        def is_cluster_ready(self):
            self.logger.info("# drivers: ready={}, total={}".format(self.ready_drivers, self.num_drivers))
            return self.ready_drivers == self.num_drivers

        def sync_drivers(self):
            self.logger.info("Sync with all drivers")
            ready_protocol = BenchmarkProtocol(SendProtocolHeader.internal_ready)
            self.manager_publisher.send_pyobj(ready_protocol)

    class WorkloadManager:
        def __init__(self, controller, cluster_manager, manual_routing_enabled=True):
            self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
            self.controller = controller
            self.cluster_manager = cluster_manager
            # we don't check this configuration for now
            # cause the implementation, i.e. node_bucket and query_bucket is inefficient
            # each retrieve operation requires notifying other node_bucket?
            self.manual_routing_enabled = manual_routing_enabled
            self.workload_db = {}
            self.current_workload_record = None
            self.current_routing_table = {}
            self.node_buckets = {}

        def set_routing_table(self, routing_table):
            self.current_routing_table = routing_table

        def get_workload_record(self, workload_id):
            return self.workload_db[str(workload_id)]

        def retrieve_requests(self, node_bucket_id, num_requests):
            # self.logger.info("Retrieve from node bucket {} for {} requests".format(node_bucket_id, num_requests))
            if len(self.node_buckets) < 1:
                # self.logger.info("Node bucket {} is not initialized".format(node_bucket_id))
                return [Request.new_empty()]
            elif node_bucket_id not in self.node_buckets:
                # self.logger.info("Node bucket {} is not empty".format(node_bucket_id))
                return [Request.new_empty()]
            else:
                node_bucket = self.node_buckets[node_bucket_id]
                # self.logger.info("Current node bucket {} has {} requests".format(node_bucket.xid, node_bucket.qsize()))
                request_list = []
                while len(request_list) < num_requests:
                    request = node_bucket.get_request()
                    #self.logger.info("Request type={}".format(request.xtype))
                    if request.xtype == RequestType.run:
                        # self.logger.info("Correcting request information")
                        workload_item = request.workload
                        workload_correct = workload.WorkloadItem(workload_item.wid, workload_item.query_name, node_bucket.endpoint, workload_item.query_key, workload_item.key)
                        request_correct = Request(request.xtype, request.xid, workload_correct)
                        request_list.append(request_correct)
                    else:
                        request_list.append(request)
                        break
                #for request in request_list:
                #    self.logger.info("xid={}, xtype={}".format(request.xid, request.xtype))
                #self.logger.info('Get {} requests from node bucket {}'.format(len(request_list)-1, node_bucket_id))
                return request_list

        def submit(self, workload):
            self.logger.info("Submit workload={}".format(workload))
            if (self.current_workload_record is not None) and (not self.current_workload_record.is_completed()):
                raise Exception("need to wait the current workload to complete")
            new_workload_id = len(self.workload_db)
            self.current_workload_record = BenchmarkController.WorkloadRecord(new_workload_id, workload)
            self.workload_db[str(new_workload_id)] = self.current_workload_record

            # here sync with all driver nodes
            self.cluster_manager.reset_ready_drivers()
            self.cluster_manager.sync_drivers()

            # start a new thread/greenlet for the newly submitted workload
            self.controller.runner_group.spawn(self.worker_dispatch_request)
            return str(new_workload_id)

        def worker_dispatch_request(self):
            self.logger.info('Dispatcher worker is running')

            # needs to reset ready_drivers to 0 before lunched this thread
            # wait for all drivers to start
            while not self.cluster_manager.is_cluster_ready():
                gevent.sleep(1)

            self.logger.info("All drrivers are ready")

            # initialize query and node buckets
            sorted_endpoints = sorted(list(set(x for sublist in self.current_routing_table.values() for x in sublist)))
            query_bucket_table = {query_name: QueryBucket(query_name) for query_name in self.current_routing_table.keys()}
            # driver id starts from 1
            node_bucket_table = {sorted_endpoints[i]: NodeBucket(i+1, sorted_endpoints[i]) for i in range(len(sorted_endpoints))}
            self.node_buckets = {node_bucket.xid: node_bucket for node_bucket in node_bucket_table.values()}
            for query_name, endpoint_list in self.current_routing_table.items():
                query_bucket = query_bucket_table[query_name]
                for endpoint in endpoint_list:
                    node_bucket = node_bucket_table[endpoint]
                    query_bucket.add_node_bucket(node_bucket)

            self.logger.info('# of query buckets: {}'.format(len(query_bucket_table)))
            self.logger.info('# of node buckets: {}'.format(len(node_bucket_table)))
            #for query_bucket in query_bucket_table.values():
            #    self.logger.info("Query bucket {}".format(query_bucket.name))
            #    for node_bucket in query_bucket.node_buckets:
            #        self.logger.info("\t* Node bucket {} => {}".format(node_bucket.xid, node_bucket))

            # start the counter
            self.current_workload_record.start()

            # start to dispatch workload - num_queries
            current_workload = self.current_workload_record.get_workload()
            for t, workload_items in current_workload.next():
                self.logger.info("current time: {}".format(t))
                self.logger.info("current workload items: {}".format(len(workload_items)))
                self.current_workload_record.add_workload(len(workload_items))
                for workload_item in workload_items:
                    query_name = workload_item.query_name
                    query_bucket = query_bucket_table[query_name]
                    #self.logger.info('wid={} {} {} {} {}'.format(workload_item.wid, workload_item.endpoint, workload_item.query_name, workload_item.query_key, workload_item.key))
                    #req = Request.new(workload_item, query_bucket)
                    req = Request.new(workload_item)
                    query_bucket.put_request(req)
                    #self.logger.info("-> query bucket {}".format(query_bucket.name))
                    #for node_bucket in query_bucket.node_buckets:
                    #    self.logger.info("\t* node bucket {} has {} request => {}".format(node_bucket.xid, node_bucket.qsize(), node_bucket))
                gevent.sleep(1)  # not good because of potential time skew

            #for query_bucket in query_bucket_table.values():
            #    for node_bucket in query_bucket.node_buckets:
            #        self.logger.info("Node bucket {} has {} requests => {}".format(node_bucket.xid, node_bucket.qsize(), node_bucket))

            #self.logger.info('-----------------')
            #for bid, node_bucket in self.node_buckets.items():
            #    self.logger.info("Node bucket {} has {} requests => {}".format(bid, node_bucket.qsize(), node_bucket))
            #for endpoint, node_bucket in self.node_bucket_table.items():
            #    self.logger.info("Node bucket {} has {} requests".format(node_bucket.xid, node_bucket.qsize()))

            for query_bucket in query_bucket_table.values():
                query_bucket.complete_dispatch()
            self.current_workload_record.completed_dispatch()

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
            self.logger.info("Workload {} is started".format(self.workload_id))
            self.time_start = time.time()

        def get_workload(self):
            return self.workload

        def add_workload(self, num_queries):
            self.query_count += num_queries

        def completed_dispatch(self):
            self.logger.info("Workload {} completed request dispatch".format(self.workload_id))
            self.dispatch_completed = True

        def report_completion(self, driver_id, report):
            self.time_last_report = time.time()
            self.num_finished_jobs += 1
            item_id = report['item']
            report.pop('item', None)
            # TODO: do we need lock??
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
        def __init__(self, controller, reporter_socket):
            self.logger = logging.getLogger(__name__)
            self.controller = controller
            self.reporter_socket = reporter_socket

        def _receive(self):
            return self.reporter_socket.recv_pyobj()

        def process(self):
            driver_id, report_statistic_list = self._receive().payloads
            self.logger.info("Received reports from driver {} for {} completions".format(driver_id, len(report_statistic_list)))
            for report_statistic in report_statistic_list:
                self.controller.workload_manager.current_workload_record.report_completion(driver_id, report_statistic)
            self.logger.debug("Driver {} completed {} reports".format(driver_id, len(report_statistic_list)))

    class BaseProtocolHandler:
        def __init__(self, target_socket):
            self.logger = logging.getLogger(__name__)
            self.target_socket = target_socket
            self.router = {}

        def process(self):
            cmd_protocol = self.target_socket.recv_pyobj()
            #self.logger.info("cmd={}".format(cmd_protocol.header))
            #self.logger.info("payload={}".format(cmd_protocol.payloads))
            try:
                result = self.router[cmd_protocol.header](*cmd_protocol.payloads)
                # self.logger.info("result=", result)
                reply_protocol = BenchmarkProtocol.new_procotol(ReceiveProtocolHeader.successful, result)
            except Exception as e:
                self.logger.exception('Failed to process command')
                #import traceback
                #traceback.print_exc()
                reply_protocol = BenchmarkProtocol(ReceiveProtocolHeader.failed)
            self.target_socket.send_pyobj(reply_protocol)

    class InternalProtocolHandler(BaseProtocolHandler):
        def __init__(self, controller, target_socket):
            super().__init__(target_socket)
            self.controller = controller
            self.router = {
                SendProtocolHeader.internal_register: self.register,
                SendProtocolHeader.internal_heartbeat: self.heartbeat,
                SendProtocolHeader.internal_ready: self.ready,
                SendProtocolHeader.internal_retrieve_jobs: self.retrieve_jobs,
            }

        def process(self):
            # self.logger.info("Driver handler is waiting for command")
            super().process()

        def register(self, driver_id):
            # decide the driver id when we start the driver, good?
            # the ready function is used to check whether the drivers are ready
            self.logger.info("Driver {} is registered".format(driver_id))
            return str(driver_id)

        def ready(self, driver_id):
            self.controller.cluster_manager.sync_from_driver(driver_id)

        def heartbeat(self, driver_id, status):
            self.controller.cluster_manager.heartbeat_from_driver(driver_id, status)

        def retrieve_jobs(self, driver_id, num_requests):
            # self.logger.info('Driver {} asks for {} request'.format(driver_id, num_requests))
            # driver_id == node_bucket_id
            return self.controller.workload_manager.retrieve_requests(driver_id, num_requests)

    class ClientProtocolHandler(BaseProtocolHandler):
        def __init__(self, controller, target_socket):
            super().__init__(target_socket)
            self.logger = logging.getLogger(__name__)
            self.controller = controller
            self.router = {
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

        def process(self):
            # self.logger.info("Client handler is waiting for command")
            super().process()

        def status(self):
            return self.controller.cluster_manager.get_cluster_status()

        def stop(self):
            stop_protocol = BenchmarkProtocol(SendProtocolHeader.admin_stop)
            self.controller.cluster_manager.manager_publisher.send_pyobj(stop_protocol)
            gevent.spawn_later(1, self.controller.stop)

        def routing_table_upload(self, routing_table):
            self.logger.info("@ routing table=%s", json.dumps(routing_table, indent=4))
            self.controller.workload_manager.set_routing_table(routing_table)

        def workload_submit(self, workload):
            self.logger.info("@ workload=%s", workload)
            # why return string?
            return str(self.controller.workload_manager.submit(workload))

        def workload_report(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).get_report()

        def workload_statistics(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).get_statistics()

        def workload_timeline_completion(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).get_timeline_completion()

        def workload_timeline_failure(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).get_timeline_failure()

        def workload_status(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).is_completed()

        def workload_download(self, workload_id):
            return self.controller.workload_manager.get_workload_record(workload_id).get_workload()

    def __init__(self, config_path):
        super(BenchmarkController, self).__init__(config_path)
        self.logger.info("Controller is initializing")

        self.num_drivers = int(self.config.lookup_config(BenchmarkConfig.DRIVER_NUM_PROCESSORS, 1) * len(self.config.get_drivers()))
        # self.num_queries = self.config.get_config("workload")["num_quries"]
        self.driver_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)
        self.reporter_port = self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORTER_PORT)

        self.cluster_manager = BenchmarkController.ClusterManager(self, self.num_drivers, self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT))
        # workload related
        self.workload_manager = BenchmarkController.WorkloadManager(self, self.cluster_manager, self.config.lookup_config(BenchmarkConfig.DRIVER_MANUAL_ROUTING, False))

    def start(self):
        self.logger.info("Controller is starting")
        self.cluster_manager.start()

        self.runner_group.spawn(self.worker_monitor)
        self.runner_group.spawn(self.worker_driver)
        self.runner_group.spawn(self.worker_collect_report)
        self.runner_group.spawn(self.worker_client)

        self.logger.info("Controller started")
        self.runner_group.join()
        self.logger.info("Controller finished")

    def stop(self):
        self.logger.info("Controller is stopped")
        self.runner_group.kill()

    def worker_client(self):
        self.logger.info("Client worker is running")
        client_socket = self.context.socket(zmq.REP)
        client_socket.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_CLIENT_PORT)))
        protocol_handler = BenchmarkController.ClientProtocolHandler(self, client_socket)

        while True:
            protocol_handler.process()

    def worker_driver(self):
        self.logger.info("Driver worker is running")
        driver_socket = self.context.socket(zmq.REP)
        driver_socket.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)))
        protocol_handler = BenchmarkController.InternalProtocolHandler(self, driver_socket)

        while True:
            protocol_handler.process()

    def worker_collect_report(self):
        self.logger.info('Report collector is running')
        reporter_socket = self.context.socket(zmq.PULL)
        reporter_socket.bind("tcp://*:{}".format(self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORTER_PORT)))

        reporter_protocol = BenchmarkController.StatisticsCollectorProtocol(self, reporter_socket)

        while True:
            reporter_protocol.process()


class BenchmarkDriver(BenchmarkNode):

    def __init__(self, config_path, node_bucket_id):
        super(BenchmarkDriver, self).__init__(config_path)
        self.logger.info("Driver initing")
        self.driver_id = node_bucket_id  # one driver per node
        self.node_bucket_id = node_bucket_id
        self.num_workers = self.config.lookup_config(BenchmarkConfig.DRIVER_NUM_WORKER)
        self.query_timeout = self.config.lookup_config(BenchmarkConfig.DRIVER_QUERY_TIMEOUT, 30)
        self.worker_pool = gevent.pool.Pool(self.num_workers)
        self.worker_queue = gevent.queue.Queue()
        self.report_queue = gevent.queue.Queue()

    def start(self):
        self.logger.info("Driver {} starting".format(self.node_bucket_id))

        # sockets initialization for communication between a driver and the controller node
        # only the controller creates server sockets
        # 1. internal communication, e.g. register, heartbeat, etc.
        # 2. work completion reporter/statistics
        # 3. manager port for publish/subscriber protocol
        self.manager_subscriber = self.context.socket(zmq.SUB)
        self.manager_subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        self.manager_subscriber.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_MANAGER_PORT)))

        # retrieve driver id first
        self.register_to_controller()

        for worker_id in range(self.num_workers):
            self.worker_pool.spawn(self.worker_query, worker_id)

        self.runner_group.spawn(self.worker_reporter)
        self.runner_group.spawn(self.worker_retrieve)
        self.runner_group.spawn(self.worker_manage)
        self.runner_group.spawn(self.worker_monitor)
        self.runner_group.spawn(self.worker_heartbeat)

        self.logger.info("Driver started")
        self.runner_group.join()

    def stop(self):
        self.worker_pool.kill()
        self.runner_group.kill()

    def monitor_health(self):
        # self.logger.info("monitoring...")
        for runner in self.runner_group.greenlets:
            if not bool(runner):
                return False
        return True

    def register_to_controller(self):
        internal_socket = self.context.socket(zmq.REQ)
        internal_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)))
        internal_protocol = BenchmarkInternalProtocol(self.driver_id, internal_socket)
        internal_protocol.register()
        internal_protocol.ready()  # for benchmark service to check

    def worker_heartbeat(self):
        '''
        Monitor the worker threads and report this node healthy
        :return:
        '''
        internal_socket = self.context.socket(zmq.REQ)
        internal_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)))
        internal_protocol = BenchmarkInternalProtocol(self.driver_id, internal_socket)
        while True:
            try:
                status = self.monitor_health()
                internal_protocol.heartbeat(status)
            except:
                import traceback
                traceback.print_exc()
            gevent.sleep(5)

    def worker_manage(self):
        internal_socket = self.context.socket(zmq.REQ)
        internal_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)))
        internal_protocol = BenchmarkInternalProtocol(self.driver_id, internal_socket)
        while True:
            # blocking call so should be good, no need to sleep
            cmd_protocol = self.manager_subscriber.recv_pyobj()
            self.logger.info("manager: %s", cmd_protocol.header)
            if cmd_protocol.header is SendProtocolHeader.admin_stop:
                self.stop()
            elif cmd_protocol.header is SendProtocolHeader.internal_echo:
                internal_protocol.echo()
            elif cmd_protocol.header is SendProtocolHeader.internal_ready:
                internal_protocol.ready()

    def worker_reporter(self):
        reporter_socket = self.context.socket(zmq.PUSH)
        reporter_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_REPORTER_PORT)))
        reporter_procotol = BenchmarkReporterProtocol(self.driver_id, reporter_socket)

        while True:
            report_statistic_list = []
            while not self.report_queue.empty():
                # should not be an issue because we have checked the queue length in the same thread
                report_statistic_list.append(self.report_queue.get())
            #TODO: add socket send here
            # report every one second
            if len(report_statistic_list) > 0:
                self.logger.info("There are {} query completed".format(len(report_statistic_list)))
                reporter_procotol.report_statistic_list(report_statistic_list)
            gevent.sleep(1)

    def worker_retrieve(self):
        '''
        The single worker that retrieves job/requests from the controller node
        :return:
        '''
        internal_socket = self.context.socket(zmq.REQ)
        internal_socket.connect("tcp://{}:{}".format(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_DRIVER_PORT)))
        internal_protocol = BenchmarkInternalProtocol(self.driver_id, internal_socket)

        while True:
            # Is this a good number? Don't take all but leave chances to other dirver??
            num_jobs_required = int(0.5 * self.num_workers)
            request_list = internal_protocol.retrieve_jobs(num_jobs_required)
            num_requests = len(request_list) if request_list[-1].xtype != RequestType.run else len(request_list)-1
            self.logger.info("Retrieved {} requests".format(num_requests))
            for request in request_list:
                #self.logger.info("Retrieved request: {}".format(request))
                if request.xtype == RequestType.empty:
                    # this should be the last element
                    #self.logger.info("Request queue is empty")
                    gevent.sleep(1)
                elif request.xtype == RequestType.stop:
                    # this should be the last element
                    #self.logger.info('job retriever completed')
                    gevent.sleep(1)
                    #return
                else:
                    workload_item = request.workload
                    workload_item.queue_timestamp = time.time()
                    self.worker_queue.put(workload_item)
            self.logger.info("current queue size: {}".format(self.worker_queue.qsize()))
            gevent.sleep(0.2)  # yield to workers?

    def worker_query(self, worker_id):
        session = query.new_session()
        while True:
            # blocking call but does not harm?
            worker_item = self.worker_queue.get()
            self.logger.info("idle workers: {}, queue lengths: {}".format(self.worker_pool.free_count(), self.worker_queue.qsize()))
            self.logger.info("worker {} is processing roxie query {}".format(worker_id, worker_item.wid))
            self.logger.info("{} {} {} {} {}".format(worker_item.wid, worker_item.endpoint, worker_item.query_name, worker_item.query_key, worker_item.key))
            time_start = time.time()
            success, output_size, status_code, exception_description = query.execute_workload_item(session, worker_item, timeout=self.query_timeout)
            time_end = time.time()

            report_detail = {
                "item": worker_item.wid,
                "queueTimestamp": worker_item.queue_timestamp,
                "startTimestamp": time_start,
                "finishTimestamp": time_end,
                "success": success,
                "size": output_size,
                "status": status_code,
                "exception": exception_description
            }
            self.report_queue.put(report_detail)



## ----------------------------- ####

class RequestGenerator:
    def __init__(self, num_requests, query_buckets):
        self.num_requests = num_requests
        self.query_buckets = query_buckets


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

            query_bucket = query_buckets[app_id-1]
            workload_item = workload.WorkloadItem(str(count), query_name, endpoint, query_key, key_firstname)
            req = Request.new(workload_item, query_bucket)
            query_bucket.put_request(req)


        for query_bucket in self.query_buckets:
            query_bucket.complete_dispatch()
        print('Completed request generator')


if __name__ == '__main__':


    num_requests = 30000
    num_query_buckets = 1024
    num_node_buckets = 8
    num_workers_per_node = 32

    query_buckets = [QueryBucket("sequential_search_firstname_{}".format(app_id)) for app_id in range(num_query_buckets)]
    node_buckets = [NodeBucket(xid) for xid in range(num_node_buckets)]
    current_index = 0

    for i in range(num_node_buckets):
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
            query_buckets[dispatcher_index].add_node_bucket(node_buckets[i])
    print('-----------')

    group = Group()
    # Request Generator
    request_generator = RequestGenerator(num_requests, query_buckets)
    group.spawn(request_generator.runner)

    # Request Worker
    worker_count = 0
    workers = []
    for node_bucket in node_buckets:
        for i in range(num_workers_per_node):
            worker = RequestWorker.new(node_bucket)
            worker_count += 1
            group.spawn(worker.runner)
            workers.append(worker)
    print('Total workers:', len(workers))

    # Node Handler
    #for node_bucket in node_buckets:
    #    group.spawn(node_bucket.runner)
    #group.spawn(completion_checker, query_buckets, node_buckets)
    group.join()

    print('=========')
    print('num of workers:', worker_count)
    print('completions:', sum([worker.count for worker in workers]))
    print('done')



