import random

from elastic.benchmark import query
from elastic.benchmark import workload

import gevent
import gevent.pool
import gevent.queue
from gevent.lock import Semaphore


num_workers = 16
worker_pool = gevent.pool.Pool(num_workers)
worker_queue = gevent.queue.Queue()
mylock = Semaphore()
results = {}
keys_exists = []


def run(output_path):
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

    endpoints = endpoints[:1]

    num_queries = num_workers * 100

    key_list = open('/home/chsu6/elastic-hpcc/benchmark/dataset/firstname_list_2000.txt', 'r').readlines()

    for app_id in range(1, 2):
        query_name = "sequential_search_firstname_{}".format(app_id)
        query_key = 'firstname'
        for key_index in range(len(key_list)):
            key_firstname = key_list[key_index].strip()
            for endpoint_index in range(len(endpoints)):
                endpoint = endpoints[endpoint_index]
                workload_item = workload.WorkloadItem("{}-{}-{}".format(endpoint_index, app_id, key_index), query_name, endpoint, query_key, key_firstname)
                worker_queue.put(workload_item)


    #print(len(worker_queue))
    for i in range(num_workers):
        worker_pool.spawn(worker, i)

    worker_pool.join()

    count_success = sum(results.values())
    print('Success:', count_success)
    print('Failure:', len(results) - count_success)

    with open(output_path, 'w') as f_out:
        for key in sorted(keys_exists):
            f_out.write(key + "\n")

    print('Complete stress testing')


def worker(index):
    session = query.new_session()

    completed = False
    while not completed:
        try:
            worker_item = worker_queue.get(timeout=1)
            success, output_size, status_code, exception_description = query.execute_workload_item(session, worker_item, timeout=120)
            mylock.acquire()
            #print(worker_item.wid, output_size)
            results[worker_item.wid] = success and output_size > 100
            if results[worker_item.wid]:
                keys_exists.append(worker_item.key)
            mylock.release()
        except Exception as e:
            #import traceback
            #traceback.print_exc()
            print('worker {} has done all the queries'.format(index))
            completed = True



if __name__ == '__main__':
    import sys
    output_path = sys.argv[1]
    run(output_path)