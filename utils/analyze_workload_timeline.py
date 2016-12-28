import pickle


def main(timeline_file):
    with open(timeline_file, 'rb') as f:
        tl = pickle.load(f)

    distribution_records = {}
    num_partitions = 1024
    num_hosts = 4
    for i in range(1, num_partitions+1):
        distribution_records[i] = 0

    for t, items in tl.timeline.items():
        for item in items:
            app_id = int(item.query_name.split('_')[-1])
            distribution_records[app_id] += 1
            #print(t, item.wid, item.endpoint, item.query_name, item.query_key, item.key)

    for i in range(1, num_partitions+1):
        print('P{}'.format(i), distribution_records[i])

    num_partitions_per_host = int(num_partitions/num_hosts)
    for i in range(num_hosts):
        count = 0
        for j in range(1, num_partitions_per_host+1):
            count += distribution_records[i*num_partitions_per_host + j]
        print('host_{}:'.format(i), count)

if __name__ == '__main__':
    import sys
    main(sys.argv[1])
