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
results = {}

def run():

    target_list = {
        'firstname':
            (
                '/home/chsu6/elastic-hpcc/benchmark/dataset/firstname_list.txt',
                '/home/chsu6/elastic-hpcc/benchmark/dataset/firstname_list_2000.txt',
                'sequential_search_firstname',
                'firstname'
            ),
        'lastname':
            (
                '/home/chsu6/elastic-hpcc/benchmark/dataset/lastname_list.txt',
                '/home/chsu6/elastic-hpcc/benchmark/dataset/lastname_list_2000.txt',
                'sequential_search_lastname',
                'lastname'
            ),
        'city':
            (
                '/home/chsu6/elastic-hpcc/benchmark/dataset/city_list.txt',
                '/home/chsu6/elastic-hpcc/benchmark/dataset/city_list_2000.txt',
                'sequential_search_city',
                'city'

            ),
        'zip':
            (
                '/home/chsu6/elastic-hpcc/benchmark/dataset/zip_list.txt',
                '/home/chsu6/elastic-hpcc/benchmark/dataset/zip_list_2000.txt',
                'sequential_search_zip',
                'zip'
            ),
    }

    endpoints = [
        "http://10.25.2.147:9876",
        "http://10.25.2.148:9876",
        "http://10.25.2.149:9876",
        "http://10.25.2.151:9876"
    ]

    index = 0
    for target in target_list.keys():
        keys_input_path, keys_output_path, query_name, query_key = target_list[target]
        results[query_key] = []
        with open(keys_input_path, 'r') as f_source:
            for key in f_source.readlines():
                key = key.strip()
                workload_item = workload.WorkloadItem(key, query_name, endpoints[index % len(endpoints)], query_key, key)
                worker_queue.put(workload_item)
                index += 1

    for i in range(num_workers):
        worker_pool.spawn(worker, i)

    worker_pool.join()

    print('Writing output file')
    for target in target_list.keys():
        keys_input_path, keys_output_path, query_name, query_key = target_list[target]
        with open(keys_output_path, 'w') as f:
            for key in sorted(results[query_key]):
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
                results[worker_item.query_key].append(worker_item.key)
            mylock.release()
        except:
            print('worker {} has done all the queries'.format(index))
            completed = True



if __name__ == '__main__':
    run()