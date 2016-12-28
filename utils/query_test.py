import random

from elastic.benchmark import query
from elastic.benchmark import workload

import gevent
import gevent.pool
import gevent.queue
from gevent.lock import Semaphore


num_workers = 128
worker_pool = gevent.pool.Pool(num_workers)
worker_queue = gevent.queue.Queue()
mylock = Semaphore()
results = {}


def run():
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

    endpoints = endpoints[:]

    num_queries = num_workers * 100

    for app_id in range(1, 1025):
        query_name = "sequential_search_firstname_{}".format(app_id)
        query_key = 'firstname'
        key_firstname = 'MARY'
        for endpoint_index in range(len(endpoints)):
            endpoint = endpoints[endpoint_index]
            workload_item = workload.WorkloadItem("{}-{}".format(app_id, endpoint_index), query_name, endpoint, query_key, key_firstname)
            worker_queue.put(workload_item)


    print(len(worker_queue))
    for i in range(num_workers):
        worker_pool.spawn(worker, i)

    worker_pool.join()

    count_success = sum(results.values())
    print('Success:', count_success)
    print('Failure:', len(results) - count_success)

    print('Complete stress testing')


def worker(index):
    session = query.new_session()

    completed = False
    while not completed:
        try:
            worker_item = worker_queue.get(timeout=1)
            success, output_size, status_code, exception_description = query.execute_workload_item(session, worker_item, timeout=120)
            mylock.acquire()
            results[worker_item.wid] = success and output_size > 100
            mylock.release()
        except Exception as e:
            #import traceback
            #traceback.print_exc()
            print('worker {} has done all the queries'.format(index))
            completed = True



if __name__ == '__main__':
    run()