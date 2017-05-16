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
            params = []  # mean, std
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


class Node:
    def __init__(self, nid, k):
        self.nid = nid
        self.slot_count = k
        self.slots = [None] * k
        self.partition_count = 0
        self.load = 0.0
        self.colors = 0
        self.partition_set = set()

    def is_full(self):
        #print("N{}: {} out of {}".format(self.nid, self.partition_count, self.slot_count))
        return self.partition_count == self.slot_count

    def add_partition(self, p):
        self.slots[self.partition_count] = p
        self.partition_set.add(p.pid)
        self.partition_count += 1
        self.load += p.load
        self.colors = len(set([self.slots[sid].pid for sid in range(self.partition_count)]))

    def contain_partition(self, p):
        return p.pid in self.partition_set

    def get_num_partitions(self, pid):
        count = 0
        for p in [s for s in self.slots if s is not None]:
            if p.pid == pid:
                count += 1
        return count


    def get_values(self):
        # load, # colors, nid
        return (self.load, self.colors, self.nid)

    def __str__(self):
        return "N{0}({1:.1f}|{2})".format(self.nid, self.load, self.colors)

    def __cmp__(self, other):
        a = self._get_values()
        b = other._get_values()
        return (a > b) - (a < b)
        #return cmp(self._get_values(), other._get_values())


class Partition:
    def __init__(self, pid, rid, load):
        self.pid = pid
        self.rid = rid
        self.load = load

    def get_values(self):
        # load, # colors, nid
        return (self.load, -self.pid, -self.rid)

    def __str__(self):
        return "P{0}({1}|{2:.1f})".format(self.pid, self.rid, self.load)


class DataPlacement:
    def __init__(self, N, k, num_replica_list, af_list):
        self.nodes = [Node(nid, k) for nid in range(1, N+1)]
        self.partitions = []
        for i in range(len(num_replica_list)):
            af_per_replica = af_list[i] / num_replica_list[i]
            pid = i + 1
            for rid in range(1, num_replica_list[i] + 1):
                self.partitions.append(Partition(pid, rid, af_per_replica))

    def mlb(self):
        #print('before:', [str(p) for p in self.partitions])
        self.partitions.sort(key=lambda x: (x.load, -x.pid, -x.rid), reverse=True)
        #print("--------------")
        #print("Partition Order")
        #print([str(p) for p in self.partitions])
        #print("--------------")

        for p in self.partitions:
            self.nodes.sort(key=lambda x: (x.load, x.nid))
            for node in self.nodes:
                #print("{} for {}".format(node, p))
                #print("N -> before:", [str(n) for n in self.nodes])
                if not node.is_full():
                    node.add_partition(p)
                    break
                #print("N -> after:", [str(n) for n in self.nodes])

        #self.nodes.sort(key=lambda x: x.get_values())
        #print([str(n) for n in self.nodes])

    def mlb_mc(self):
        self.partitions.sort(key=lambda x: (x.load, -x.pid, -x.rid), reverse=True)

        for p in self.partitions:
            self.nodes.sort(key=lambda x: (x.load, x.colors, x.nid))
            for node in self.nodes:
                if not node.is_full():
                    node.add_partition(p)
                    break

    def mc_mlb(self):
        self.partitions.sort(key=lambda x: (x.load, -x.pid, -x.rid), reverse=True)

        for p in self.partitions:
            # case 1: sort by num of colors per node but skip the nodes with the same partition id
            self.nodes.sort(key=lambda x: (x.colors, x.load, x.nid))
            added = False
            for node in [n for n in self.nodes if not n.contain_partition(p)]:
                if not node.is_full():
                    node.add_partition(p)
                    added = True
                    break
            # case 2: all nodes contain the same partition id then sort by number of colors, load and nid
            if not added:
                self.nodes.sort(key=lambda x: (x.get_num_partitions(p.pid), x.load, x.nid))
                for node in self.nodes:
                    if not node.is_full():
                        node.add_partition(p)
                        break

    def mc(self):
        self.partitions.sort(key=lambda x: (x.load, -x.pid, -x.rid), reverse=True)

        for p in self.partitions:
            self.nodes.sort(key=lambda x: (x.get_num_partitions(p.pid), x.load, x.nid))
            for node in self.nodes:
                if not node.is_full():
                    node.add_partition(p)
                    break

    def ml(self):
        '''
        maximize data locality / minimize memory footprint
        '''
        self.partitions.sort(key=lambda x: (x.load, -x.pid, -x.rid), reverse=True)

        for p in self.partitions:
            self.nodes.sort(key=lambda x: (x.get_num_partitions(p.pid), -x.load, -x.nid), reverse=True)
            for node in self.nodes:
                if not node.is_full():
                    node.add_partition(p)
                    break

    def print_details(self):
        nodes = [n for n in self.nodes]
        nodes.sort(key=lambda x: x.nid)

        for node in nodes:
            print("N{} ({})\t{}".format(
                node.nid,
                "{0:.1f}".format(node.load),
                "\t".join([str(p) for p in node.slots])
            ))

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

    print("Colors")
    for node_id, partition_list in dp_records.items():
        print("N{}: {}".format(node_id+1, len(set(partition_list))))

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


def generate_minimum_replica_list(af_list, S):
    num_replicas_list = calculate_num_replicas(S, af_list)
    adjusted_num_replicas_list = [round(n) for n in num_replicas_list]
    adjusted_num_replicas_list = [n if n > 0 else 1 for n in adjusted_num_replicas_list]
    #adjusted_weight_list = [num_replicas_list[i] - adjusted_num_replicas_list[i] for i in range(len(num_replicas_list))]
    return adjusted_num_replicas_list


def adjust_num_replicas_by_weight(af_list, replica_list, num_slots):
    weight_list = [af_list[i] / replica_list[i] for i in range(len(af_list))]
    # this should be the only case?
    while sum(replica_list) < num_slots:
        # the highest will be zero for its index
        sorted_index = sorted(range(len(weight_list)), key=lambda k: -weight_list[k])
        max_index = sorted_index.index(0)
        replica_list[max_index] += 1
        weight_list[max_index] = af_list[max_index] / replica_list[max_index]

    while sum(replica_list) > num_slots:
        min_value = float('inf')
        # the lowest will be zero for its index
        sorted_index = sorted(range(len(weight_list)), key=lambda k: weight_list[k])
        #print('inside loop')
        #print(replica_list)
        #print(sorted_index)
        min_index = -1
        for index in sorted_index:
            if replica_list[index] <= 1:
                continue
            else:
                min_index = index
                break
        #print("select", min_index)
        replica_list[min_index] -= 1
        weight_list[min_index] = af_list[min_index] / replica_list[min_index]

    # fix a possible issue?
    return [int(x) for x in replica_list]


def run(M, N, k, t, workload_name='uniform', af_list=[], show_output=True):
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


    def dp_rainbow2(k, num_nodes, num_replicas_list):
        current_node_id = 0
        dp_records = defaultdict(lambda: [])
        for replica_index in range(len(num_replicas_list)):
            for num_replica in range(num_replicas_list[replica_index]):
                # both starts from 1
                dp_records[current_node_id % num_nodes].append(replica_index)
                current_node_id += 1
        return dp_records

    def dp_rainbow(k, num_nodes, num_replicas_list, weight_list):
        sorted_index = sorted(range(len(weight_list)), key=lambda k: -weight_list[k])
        current_node_id = 0
        dp_records = defaultdict(lambda: [])
        for replica_index in sorted_index:
            for num_replica in range(num_replicas_list[replica_index]):
                # both starts from 1
                dp_records[current_node_id % num_nodes].append(replica_index)
                #print("P{} -> N{}".format(replica_index+1, current_node_id % num_nodes))
                current_node_id += 1
        return dp_records

    def dp_monochromatic(k, num_nodes, num_replicas_list, weight_list):
        #print("replica_list", num_replicas_list)
        #print("weight_list", weight_list)
        sorted_index = sorted(range(len(weight_list)), key=lambda k: -weight_list[k])
        #print("sorted_index", sorted_index)
        dp_records = {}
        for i in range(num_nodes):
            dp_records[i] = []

        for replica_index in sorted_index:
            #print('\treplica_index', replica_index)
            num_replicas_remaining = num_replicas_list[replica_index]
            #print('\tremaining', num_replicas_remaining)
            while num_replicas_remaining > 0:
                #print(json.dumps(dp_records, indent=4))
                sorted_index_by_free_slots = sorted(range(len(dp_records)), key=lambda z: (-(k-len(dp_records[z])), sum([weight_list[replica_index] for replica_index in dp_records[z]])))
                #print('\t\t# free_slot_index', sorted_index_by_free_slots)
                for node_index in sorted_index_by_free_slots:
                    #print("\t\t\tnode_index", node_index)
                    #print("\t\t\t$ actual remaining", num_replicas_remaining)
                    if num_replicas_remaining <= 0:
                        break
                    num_free_slots = k-len(dp_records[node_index])
                    #print('\t\t\t@ free_slots', num_free_slots)
                    if num_free_slots >= num_replicas_remaining:
                        for i in range(num_replicas_remaining):
                            #print('\t\t\t\tputting...')
                            dp_records[node_index].append(replica_index)
                            num_replicas_remaining -= 1
                            #print(json.dumps(dp_records, indent=4))
                        break
                    else:
                        for i in range(num_free_slots):
                            dp_records[node_index].append(replica_index)
                            num_replicas_remaining -= 1
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

    minimum_replicas_list = generate_minimum_replica_list(af_list, S)
    # expected load per partition
    minimum_weight_list = [af_list[i] / minimum_replicas_list[i] for i in range(len(af_list))]

    if show_output:
        print('before)')
        print("num_replicas:", _to_string(minimum_replicas_list, is_integer=True))
        print("weight:", _to_string(minimum_weight_list))

    adjusted_replicas_list = adjust_num_replicas_by_weight(af_list, minimum_replicas_list, S)
    adjusted_weight_list = [af_list[i] / adjusted_replicas_list[i] for i in range(len(af_list))]

    if show_output:
        print('after)')
        print("num_replicas:", _to_string(adjusted_replicas_list, is_integer=True))
        print("weight:", _to_string(adjusted_weight_list))
        print("-----------------------------------------")
    dp_records = {}
    if t == 'mlb':
        dp = DataPlacement(N, k, adjusted_replicas_list, af_list)
        dp.mlb()
        dp_records = convert_dp_to_legacy_format(dp)
    elif t in ['rainbow', 'mc']:
        #dp_records = dp_rainbow(k, N, adjusted_replicas_list, adjusted_weight_list)
        dp = DataPlacement(N, k, adjusted_replicas_list, af_list)
        dp.mc()
        dp_records = convert_dp_to_legacy_format(dp)
    elif t in ['monochromatic', 'ml']:
        # dp_records = dp_monochromatic(k, N, adjusted_replicas_list, adjusted_weight_list)
        dp = DataPlacement(N, k, adjusted_replicas_list, af_list)
        dp.ml()
        dp_records = convert_dp_to_legacy_format(dp)
    elif t in ['mcmlb']:
        dp = DataPlacement(N, k, adjusted_replicas_list, af_list)
        dp.mc_mlb()
        dp_records = convert_dp_to_legacy_format(dp)

    if show_output:
        #print(json.dumps(dp_records, indent=4, sort_keys=True))
        _print_placement(dp_records, adjusted_replicas_list, af_list)
    return dp_records, adjusted_replicas_list
    #print(json.dumps(dp_records, indent=4, sort_keys=True))


def convert_dp_to_legacy_format(dp):
    nodes = [n for n in dp.nodes]
    nodes.sort(key=lambda x: x.nid)

    dp_records = {}
    for node in nodes:
        partitinos = [p for p in node.slots]
        partitinos.sort(key=lambda x: x.pid)

        dp_records[node.nid-1] = [p.pid - 1 for p in partitinos]
    return dp_records

if __name__ == "__main__":
    M = 4  # minimum cluster size
    N = 8  # number of nodes
    k = 2  # number of maximum partitions per node, coarse: k=1, fine: k>=1
    af_list = [17, 13, 4, 1]  # partition access frequency, len(paf) = m*k
    run(M, N, k, af_list, 'mcs')
