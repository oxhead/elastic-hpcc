from elastic.benchmark import query
from elastic.benchmark import workload

import gevent
import gevent.pool
import gevent.queue
from gevent.lock import Semaphore


num_workers = 1
worker_pool = gevent.pool.Pool(num_workers)
worker_queue = gevent.queue.Queue()
mylock = Semaphore()
results = {}


def run():
    endpoints = [
        "http://10.25.2.147:9876",
        "http://10.25.2.148:9876",
        "http://10.25.2.149:9876",
        "http://10.25.2.151:9876",
        "http://10.25.2.152:9876",
        "http://10.25.2.153:9876",
        "http://10.25.2.157:9876",
        "http://10.25.2.131:9876"
    ]
    #endpoints = endpoints[:2]

    num_queries = num_workers * 100

    index = 0
    query_name = "sequential_search_firstname"
    query_key = 'firstname'
    key = 'MARY'
    print('total queries={}'.format(num_queries))
    for i in range(num_queries):
        workload_item = workload.WorkloadItem(query_key, query_name, endpoints[index % len(endpoints)], query_key, key)
        worker_queue.put(workload_item)
        index += 1

    print(len(worker_queue))
    for i in range(num_workers):
        worker_pool.spawn(worker, i)

    worker_pool.join()

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