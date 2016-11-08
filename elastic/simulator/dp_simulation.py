import enum
import math
from collections import defaultdict
import json
import copy

import numpy as np
from scipy import stats

from elastic.util import collection as collection_helper


class WorkloadGenerator:
    @staticmethod
    def new(workload_name, size=16):
        distribution_type = workload_name.lower()
        n_samples = 1000000  # should be accurate enough

        params = []
        statistics_distribution = None
        if distribution_type == 'uniform':
            params = []
            statistics_distribution = stats.uniform;
        elif distribution_type == 'normal':
            params = []
            statistics_distribution = stats.norm
        elif distribution_type == 'beta':
            params = [2, 5]
            statistics_distribution = stats.beta
        elif distribution_type == 'gamma':
            params = [2]
            statistics_distribution = stats.gamma
        elif distribution_type == 'powerlaw':
            params = [5]
            statistics_distribution = stats.powerlaw
        else:
            raise Exception("Undefined distribution type")

        num_chunks = size
        x_min = statistics_distribution.ppf(0.0000001, *params)
        x_max = statistics_distribution.ppf(0.9999999, *params)
        x_list = [x_min + (x_max - x_min) / num_chunks * i for i in range(1, num_chunks + 1)]
        cdf_list = statistics_distribution.cdf(x_list, *params)
        cdf_list = np.append([0], cdf_list)
        pdf_list = sorted(np.diff(cdf_list), reverse=True)
        probability_list = pdf_list
        for i in range(len(probability_list) - 1):
            probability_list[i + 1] += probability_list[i]
        probability_list[-1] = 1.0
        # print(probability_list)
        # cdf_diff = np.diff(cdf_list)
        # print('before)', cdf_list)
        # reversed_cdf_list = list(reversed([abs(n-1) for n in cdf_list]))
        # print('after)', list(reversed_cdf_list))
        return np.diff([0] + probability_list)

def calculate_partition_loads(num_replicas_list, af_list):
    partition_load_records = [af_list[partition_id] / num_replicas_list[partition_id] for
                              partition_id
                              in range(len(num_replicas_list))]
    return partition_load_records


def calculate_node_loads(dp_records, partition_load_records):
    node_load_records = defaultdict(lambda: 0)
    for node_id, partition_list in dp_records.items():
        for partition_id in partition_list:
            node_load_records[node_id] += partition_load_records[partition_id]
    return node_load_records


def _print_placement(dp_records, num_replicas_list, af_list):
    partition_load_records = calculate_partition_loads(num_replicas_list, af_list)
    node_load_records = defaultdict(lambda: 0)
    for node_id, partition_list in dp_records.items():
        for partition_id in partition_list:
            node_load_records[node_id] += partition_load_records[partition_id]
    for node_id in sorted(dp_records.keys()):
        print("N{} ({})\t{}".format(node_id+1, "{0:.{1}f}".format(node_load_records[node_id], 2), " ".join(
            ["P{}".format(replica_index+1) for replica_index in sorted(dp_records[node_id])])))

        # for replica_index in sorted(dp_records[node_id]):
        #    print("\tP{}".format(replica_index))

def _to_string(n_list, precision=2, is_integer=False):
    if is_integer:
        return "[{}]".format(', '.join(["{:d}".format(int(n)) for n in n_list]))
    else:
        return "[{}]".format(', '.join(["{0:.{1}f}".format(n, precision) for n in n_list]))
    # return ["{0:.{1}f}".format(n, precision) for n in n_list]


def calculate_num_replicas(S, af_list):
    # print("@", S)
    total_load = sum(af_list)
    num_replica_list = [af / total_load * S for af in af_list]
    return num_replica_list


def adjust_num_replicas(num_replicas_list):
    adjusted_num_replicas_list = [math.floor(n) for n in num_replicas_list]
    adjusted_num_replicas_list = [n if n > 0 else 1 for n in adjusted_num_replicas_list]
    adjusted_weight_list = [num_replicas_list[i] - adjusted_num_replicas_list[i] for i in range(len(num_replicas_list))]
    return adjusted_num_replicas_list, adjusted_weight_list


def adjust_num_replicas_by_weight(adjusted_num_replicas_list, adjusted_weight_list, num_slots):
    # this should be the only case?
    while sum(adjusted_num_replicas_list) < num_slots:
        max_value = max(adjusted_weight_list)
        max_index = adjusted_weight_list.index(max_value)
        adjusted_num_replicas_list[max_index] += 1
        adjusted_weight_list[max_index] -= 1

    while sum(adjusted_num_replicas_list) > num_slots:
        min_value = float('inf')
        for i in range(len(adjusted_num_replicas_list)):
            if adjusted_weight_list[i] < min_value and adjusted_num_replicas_list[i] > 1:
                min_value = adjusted_weight_list[i]
                min_index = i
        adjusted_num_replicas_list[min_index] -= 1
        adjusted_weight_list[min_index] += 1

    return adjusted_num_replicas_list, adjusted_weight_list


def run(M, N, k, t, workload_name='uniform', af_list=[]):
    '''
    Algorithm:
    0. Assumptions
        1) knows the workload (access frequency)
        2) knows the node capacity (maximum load)
        3) workload characterization remains similar for a period of time
    1. Parameters
        1) M: minimum number of nodes
        2) N: number of nodes
        3) k: number of equal-sized partitions per node
        4) P = Mk: number of unique partitions
        5) S = Nk: number of slots
        6) a_i: access frequency for i <= |P|
        7) A: total load a cluster to serve
    2. optimize objective functions
        1) maximize color span
        2) minimize color span (exploit cache locality)
        3) maximize load balanced
        4) minimize number of newly replicated partitions
        5) minimize movement cost
        6) load-aware
    3. Approaches
        1) determine the number of replicas first
        2) assumptions of maximum load capacity
    3. Procedure
        1) determine the number of replicas (based on workload or movement?)
            * calculate by paf/sum(paf) * o*k
            * if overflow: decreases


    :return:
    '''




    def dp_maximize_color_span(k, num_nodes, num_replicas_list):
        current_node_id = 0
        dp_records = defaultdict(lambda: [])
        for replica_index in range(len(num_replicas_list)):
            for num_replica in range(num_replicas_list[replica_index]):
                # both starts from 1
                dp_records[current_node_id % num_nodes].append(replica_index)
                current_node_id += 1
        return dp_records

    def dp_maximize_load_balanced(k, num_nodes, num_replicas_list, af_list):
        # obviously not optimized
        # expand the replica list from number to elements
        partition_load_records = calculate_partition_loads(num_replicas_list, af_list)
        #print('## pr:', partition_load_records)
        replica_candidates = []
        for i in range(len(num_replicas_list)):
            for j in range(num_replicas_list[i]):
                replica_candidates.append(i)
        #print(replica_candidates)
        dp_records = {node_id: [] for node_id in range(num_nodes)}
        while len(replica_candidates) > 0:
            #print("# remaining:", len(replica_candidates))
            # pick the smalled load first
            node_load_records = calculate_node_loads(dp_records, partition_load_records)
            min_value = float('inf')
            min_index = -1
            for i in range(num_nodes):
                if node_load_records[i] < min_value and len(dp_records[i]) < k:
                    min_index = i
                    min_value = sum([partition_load_records[n] for n in dp_records[i]])
            #print('min node index:', min_index)
            # prepare necessary data
            node_load_list = [node_load_records[i] for i in range(num_nodes)]
            best_balanced_load = float('inf')
            best_index = -1
            for i in range(len(replica_candidates)):
                new_load = node_load_list[min_index]
                load_diff = sum([abs(new_load-node_load_list[n]) for n in range(num_nodes) if n != min_index])
                if load_diff < best_balanced_load:
                    best_index = i;
            dp_records[min_index].append(replica_candidates[best_index])
            del replica_candidates[best_index]
        return dp_records

    def generate_workload(workload_name, size=16):
        return WorkloadGenerator.new(workload_name, size)

    print("M:", M)
    print("N:", N)
    print("k:", k)
    af_list = af_list if len(af_list) > 0 else generate_workload(workload_name=workload_name, size=M*k)
    print("workload:", af_list)
    print("objective:", t)
    S = N * k

    # calculate the weight
    num_replicas_list = calculate_num_replicas(S, af_list)
    adjusted_num_replicas_list, adjusted_weight_list = adjust_num_replicas(num_replicas_list)

    print("original weight:", _to_string(num_replicas_list))
    print('before)')
    print("num_replicas:", _to_string(adjusted_num_replicas_list, is_integer=True))
    print("weight:", _to_string(adjusted_weight_list))
    adjusted_num_replicas_list, adjusted_weight_list = adjust_num_replicas_by_weight(adjusted_num_replicas_list, adjusted_weight_list, S)
    print('after)')
    print("num_replicas:", _to_string(adjusted_num_replicas_list, is_integer=True))
    print("weight:", _to_string(adjusted_weight_list))
    print("-----------------------------------------")
    dp_records = {}
    if t == 'mcs':
        dp_records = dp_maximize_color_span(k, N, adjusted_num_replicas_list)
    elif t == 'mlb':
        dp_records = dp_maximize_load_balanced(k, N, adjusted_num_replicas_list, af_list)
    _print_placement(dp_records, adjusted_num_replicas_list, af_list)
    #print(json.dumps(dp_records, indent=4, sort_keys=True))




if __name__ == "__main__":
    M = 4  # minimum cluster size
    N = 8  # number of nodes
    k = 2  # number of maximum partitions per node, coarse: k=1, fine: k>=1
    af_list = [17, 13, 4, 1]  # partition access frequency, len(paf) = m*k
    run(M, N, k, af_list, 'mcs')
