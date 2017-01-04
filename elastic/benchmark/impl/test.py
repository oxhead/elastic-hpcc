import logging
import random
import json


import zmq.green as zmq
import gevent

from elastic import init
from elastic.benchmark.impl.protocol import *
from elastic.benchmark.workload import *


if __name__ == '__main__':
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="client")

    context = zmq.Context()
    client_socket = context.socket(zmq.REQ)
    client_socket.connect("tcp://{}:{}".format('localhost', '9999'))

    client_protocol = BenchmarkClientProtocol(client_socket)

    # 1. test status
    print(client_protocol.status())

    # upload routing table
    routing_table = {
        'sequential_search_firstname_1': ['http://10.25.2.131:9876'],
        'sequential_search_firstname_2': ['http://10.25.2.132:9876'],
    }
    client_protocol.routing_table_upload(routing_table)

    # submit workload
    num_queries = 10
    workload_timeline = {}
    workload_timeline[0] = []
    query_name_list = list(routing_table.keys())
    endpoint_list = list(set(x for sublist in routing_table.values() for x in sublist))
    for i in range(num_queries):
        workload_item = WorkloadItem(str(i), random.choice(query_name_list), random.choice(endpoint_list), 'firstname', 'MARY')
        print(workload_item.endpoint, workload_item.query_name, workload_item.query_key, workload_item.key)
        workload_timeline[0].append(workload_item)
    workload_id = client_protocol.workload_submit(WorkloadExecutionTimeline(1, workload_timeline))

    while not client_protocol.workload_status(workload_id):
        print('workload {} is not completed'.format(workload_id))
        gevent.sleep(2)
    workload_statistics = client_protocol.workload_statistics(workload_id)
    print(json.dumps(workload_statistics, indent=4))
    # stop service
    #client_protocol.stop()
