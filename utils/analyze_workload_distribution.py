import json

def calculate_host_partition_access(workload_distribution):
    partition_access_per_host = {i: 0 for i in range(1, 5)}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            partition_number = int(partition.split('.')[-1].split('_')[1])
            partition_access_per_host[partition_number] += access
    print(json.dumps(partition_access_per_host, indent=4, sort_keys=True))

def calculate_file_partition_access(workload_distribution):
    partition_access = {}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            if partition not in partition_access:
                partition_access[partition] = 0
            partition_access[partition] += access

    print(json.dumps(partition_access, indent=4, sort_keys=True))

input_files = {
    "uniform": "/home/chsu6/elastic-hpcc/mybenchmark/E18/placement/uniform.json",
    "powerlaw": "/home/chsu6/elastic-hpcc/mybenchmark/E18/placement/powerlaw.json",
    "normal": "/home/chsu6/elastic-hpcc/mybenchmark/E18/placement/normal.json",
    "beta": "/home/chsu6/elastic-hpcc/mybenchmark/E18/placement/beta.json",
    "gamma": "/home/chsu6/elastic-hpcc/mybenchmark/E18/placement/gamma.json",
}

for distribution_type, input_file in input_files.items():
    with open(input_file, 'r') as f:
        workload_distribution = json.load(f)
    print(distribution_type, input_file)
    calculate_host_partition_access(workload_distribution)




