from elastic.benchmark import query
from elastic.benchmark import workload

import gevent
import gevent.pool
import gevent.queue
from gevent.lock import Semaphore


num_workers = 32
worker_pool = gevent.pool.Pool(32)
worker_queue = gevent.queue.Queue()
mylock = Semaphore()
results = []

def run():

    file_path = '/home/chsu6/elastic-hpcc/benchmark/dataset/firstname_list.txt'
    output_path = 'firstname_list_filtered.txt'
    query_name = "sequential_search_firstname"
    query_key = 'firstname'


    endpoints = [
        "http://10.25.2.147:9876",
        "http://10.25.2.148:9876",
        "http://10.25.2.149:9876",
        "http://10.25.2.151:9876"
    ]

    index = 0
    with open(file_path, 'r') as f_source:
        for key in f_source.readlines():
            key = key.strip()
        #for key in ['MARY']:
            workload_item = workload.WorkloadItem(key, query_name, endpoints[index % len(endpoints)], query_key, key)
            worker_queue.put(workload_item)
            index += 1

    for i in range(num_workers):
        worker_pool.spawn(worker, i)

    worker_pool.join()

    print('Write output file')
    with open(output_path,'w') as f:
        for key in sorted(results):
            f.write("{}\n".format(key))



def worker(index):
    session = query.new_session()

    completed = False
    while not completed:
        try:
            worker_item = worker_queue.get(timeout=10)
            success, output_size = query.execute_workload_item(session, worker_item, timeout=10)
            mylock.acquire()
            if success and output_size > 100:
                results.append(worker_item.key)
            mylock.release()
        except:
            print('worker {} has done all the queries'.format(index))
            completed = True



if __name__ == '__main__':
    run()