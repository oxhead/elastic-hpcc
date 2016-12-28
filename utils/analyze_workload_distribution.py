import json
import sys
import glob
import os

def calculate_host_partition_access(workload_distribution):
    partition_access_per_host = {}
    for host, records in workload_distribution.items():
        partition_access_per_host[host] = sum(records.values())
    print(json.dumps(partition_access_per_host, indent=4, sort_keys=True))

def calculate_4host_partition_access(workload_distribution):
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

def calculate_aggregate_file_partition_access(workload_distribution):
    partition_access = {}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            partition_number = partition.split('.')[-1].split('_')[1]
            if partition_number not in partition_access:
                partition_access[partition_number] = 0
            partition_access[partition_number] += access

    print(json.dumps(partition_access, indent=4, sort_keys=True))


def calculate_aggregate_app_access(workload_distribution):
    partition_access = {}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            partition_number = partition.split('.')[0].split('_')[-1]
            if partition_number not in partition_access:
                partition_access[partition_number] = 0
            partition_access[partition_number] += access

    print(json.dumps(partition_access, indent=4, sort_keys=True))


def calculate_sudo_host_access(workload_distribution, num_hosts=4, num_partitions=1024):
    num_partitions_per_host = num_partitions / num_hosts
    partition_access_per_host = {k: 0 for k in range(1, num_hosts+1)}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            app_id = int(partition.split('/')[-1].split('.')[0].split('_')[-1])
            host_id = int((app_id-1) / num_partitions_per_host) + 1
            try:
                partition_access_per_host[host_id] += access
            except:
                print(app_id, host_id, num_partitions_per_host)
    print(json.dumps(partition_access_per_host, indent=4, sort_keys=True))


def calculate_partition_distribution(workload_distribution, num_hosts=4, num_partitions=1024):
    partition_access = {}
    for host, records in workload_distribution.items():
        for partition, access in records.items():
            if partition not in partition_access:
                partition_access[partition] = 0
            partition_access[partition] += access

    print(json.dumps(partition_access, indent=4, sort_keys=True))


def show_data(distribution_type, input_file):
    with open(input_file, 'r') as f:
        workload_distribution = json.load(f)
    print(distribution_type, input_file)
    #calculate_host_partition_access(workload_distribution)
    #calculate_file_partition_access(workload_distribution)
    #calculate_aggregate_file_partition_access(workload_distribution)
    #calculate_aggregate_app_access(workload_distribution)
    calculate_sudo_host_access(workload_distribution, num_hosts=4, num_partitions=1024)
    #calculate_partition_distribution(workload_distribution)

if __name__ == "__main__":
    dir_path = sys.argv[1]
    print(dir_path)
    for result_dir in sorted(glob.glob("{}/*".format(dir_path))):
        input_file = os.path.join(result_dir, "result", "access_distribution.json")
        show_data(result_dir, input_file)






