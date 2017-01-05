import collections
import json
import copy
import math
import enum

from elastic.util import helper


class DataPlacementType(enum.Enum):
    coarse_partial = 1
    coarse_all = 2
    fine_partial = 3
    fine_all = 4
    complete = 5


class PlacementTool:
    @staticmethod
    def load_statistics(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def extract_partitions(locations):
        partitions = set()
        for node, partition_list in locations.items():
            partitions.update(partition_list)
        return list(partitions)

    @staticmethod
    def compute_partition_statistics(access_statistics):
        statistics = collections.defaultdict(lambda : 0)
        for node, partition_statistics in access_statistics.items():
            for partition, count in partition_statistics.items():
                statistics[partition] += count
        return dict(statistics)

    @staticmethod
    def compute_node_statistics(access_statistics):
        statistics = collections.defaultdict(lambda : 0)
        for node, partition_statistics in access_statistics.items():
            for partition, count in partition_statistics.items():
                statistics[node] += count
        return dict(statistics)

    @staticmethod
    def convert_to_node_statistics(locations, access_statistics):
        statistics = collections.defaultdict(lambda : 0)
        partition_statistics = PlacementTool.compute_partition_statistics(access_statistics)
        for node, partition_list in locations.items():
            for partition in partition_list:
                for partition_compare, count in partition_statistics.items():
                    if helper.simple_string_match(partition, partition_compare):
                        statistics[node] += count
        return dict(statistics)


class DataPlacement:
    @staticmethod
    def new(locations, name=None):
        old_locations = locations
        old_partitions = PlacementTool.extract_partitions(locations)
        old_nodes = list(locations.keys())
        return DataPlacement(old_nodes, old_partitions, old_locations, name=name)

    def __init__(self, nodes, partitions, locations, name=None):
        self.nodes = nodes
        self.partitions = partitions
        self.locations = locations
        self.name = name


class CompleteDataPlacement:
    @staticmethod
    def compute_optimal_placement(old_placement, new_nodes, name=None):
        all_nodes = list(set(old_placement.nodes + new_nodes))
        all_locations = {n: old_placement.partitions for n in all_nodes}
        return DataPlacement(all_nodes, old_placement.partitions, all_locations, name=name)


class CoarseGrainedDataPlacement:
    @staticmethod
    def compute_optimal_placement(old_placement, new_nodes, access_statistics):

        nodes_to_add = sorted(list(set(new_nodes) - set(old_placement.nodes)))
        #print(nodes_to_add)
        DataPlacement(new_nodes, old_placement.partitions, new_locations)
        node_statistics = PlacementTool.convert_to_node_statistics(old_placement.locations, access_statistics)
        #print("node_statistics:", node_statistics)

        total_access = sum(node_statistics.values())
        node_statistics_percentage = {node: count/total_access for node, count in node_statistics.items()}
        #print(node_statistics_percentage)


        node_number_allocation = {node: round(percentage * len(nodes_to_add)) for node, percentage in node_statistics_percentage.items()}
        #print(node_number_allocation)

        node_allocation_sorted = sorted(node_number_allocation.items(), key=lambda x: x[1], reverse=True)
        #print("before:", node_allocation_sorted)

        satisfied = False
        need_increase = sum([allocation for _, allocation in node_allocation_sorted]) < len(nodes_to_add)
        current_index = 0 if need_increase else len(node_allocation_sorted) - 1
        while not satisfied:
            if sum([allocation for _, allocation in node_allocation_sorted]) == len(nodes_to_add):
                satisfied = True
            else:
                if need_increase:
                    node, allocation = node_allocation_sorted[current_index%len(node_allocation_sorted)]
                    allocation += 1
                    node_allocation_sorted[current_index] = (node, allocation)
                    current_index += 1
                else:
                    node, allocation = node_allocation_sorted[current_index % len(node_allocation_sorted)]
                    allocation -= 1
                    node_allocation_sorted[current_index] = (node, allocation)
                    current_index -= 1

        #print("after:", node_allocation_sorted)

        old_locations = old_placement.locations
        new_locations = {}
        current_index = 0
        for node, allocation in node_allocation_sorted:
            for i in range(allocation):
                new_locations[nodes_to_add[current_index]] = old_locations[node]
                current_index += 1

        new_locations.update(old_locations)
        #print("optimal:", json.dumps(new_locations, indent=4, sort_keys=True))

        return


class FineGrainedDataPlacement:
    @staticmethod
    def compute_optimal_placement(old_placement, new_nodes, access_statistics):
        #print('computing...........')
        #print(json.dumps(old_placement.locations, indent=4, sort_keys=True))
        #print(new_nodes)
        #print(json.dumps(access_statistics, indent=4, sort_keys=True))
        nodes_to_add = sorted(list(set(new_nodes) - set(old_placement.nodes)))
        #print(nodes_to_add)
        partition_statistics = {}
        for node, partition_list in access_statistics.items():
            for partition, count in partition_list.items():
                if partition not in partition_statistics:
                    partition_statistics[partition] = 0
                partition_statistics[partition] += count
        #print("statistics:", json.dumps(partition_statistics, indent=4, sort_keys=True))

        total_acess = sum(partition_statistics.values())
        partition_statistics_percentage = {partition: count/total_acess for partition, count in partition_statistics.items()}

        #print("statistics:", json.dumps(partition_statistics_percentage, indent=4, sort_keys=True))

        num_allocation = math.ceil(len(nodes_to_add)/len(old_placement.nodes) * sum([len(partition_list) for partition_list in old_placement.locations.values()]))
        #print("@@", num_allocation)

        #print('Target allocations:', num_allocation)
        max_partition_per_node = math.ceil(num_allocation / len(nodes_to_add))
        max_allocation_per_partition = len(nodes_to_add)
        partition_allocation = {partition: round(num_allocation*percentage) for partition, percentage in partition_statistics_percentage.items()}

        #print("allocation:", json.dumps(partition_allocation, indent=4, sort_keys=True))

        partition_allocation_sorted = sorted(partition_allocation.items(), key=lambda x: partition_statistics_percentage[x[0]], reverse=True)
        #print('sorted allocation:', partition_allocation_sorted)

        partition_allocation_adjusted = copy.copy(partition_allocation_sorted)
        current_index = 0

        # Adjust to fit the maximum allocations
        for i in range(len(partition_allocation_sorted)):
            partition, count = partition_allocation_sorted[i]
            if count > max_allocation_per_partition:
                partition_allocation_adjusted[i] = (partition, max_allocation_per_partition)


        #print('1) adjusted allocation:')
        #for partition, count in partition_allocation_adjusted:
        #    print(count, partition)


        '''
        Allocation by access percentage may not fit to the total sum at all
        Case 1: < maximum total allocations -> increase from the very top one
        Case 2: > maximum total allocations -> decrease from the very down one
        '''
        satisfied = False
        while not satisfied:
            current_allocation = sum([count for _, count in partition_allocation_adjusted])
            #print("### current", current_allocation)
            if current_allocation == num_allocation:
                satisfied = True
            elif current_allocation < num_allocation:
                for i in range(len(partition_allocation_adjusted)):
                    if partition_allocation_adjusted[i][1] >= max_allocation_per_partition:
                        continue
                    else:
                        current_allocation = sum([count for _, count in partition_allocation_adjusted])
                        if current_allocation >= num_allocation:
                            break
                        else:
                            #print("(1)", partition_allocation_adjusted[i])
                            partition, count = partition_allocation_adjusted[i]
                            partition_allocation_adjusted[i] = (partition, count + 1)
                            #print("(2)", partition_allocation_adjusted[i])

            elif current_allocation > num_allocation:
                for i in reversed(range(len(partition_allocation_adjusted))):
                    if partition_allocation_adjusted[i][1] <= 1:
                        continue
                    else:
                        current_allocation = sum([count for _, count in partition_allocation_adjusted])
                        if current_allocation <= num_allocation:
                            break
                        else:
                            # print("(1)", partition_allocation_adjusted[i])
                            partition, count = partition_allocation_adjusted[i]
                            partition_allocation_adjusted[i] = (partition, count - 1)
                            # print("(2)", partition_allocation_adjusted[i])

        #print('2) adjusted allocation:')
        #for partition, count in partition_allocation_adjusted:
        #    print(count, partition)

        new_locations = {node: [] for node in nodes_to_add}
        current_node_index = 0
        for partition, count in partition_allocation_adjusted:
            for i in range(count):
                new_locations[nodes_to_add[current_node_index%len(nodes_to_add)]].append(partition)
                current_node_index += 1

        new_locations.update(old_placement.locations)
        #print("optimal:", json.dumps(new_locations, indent=4, sort_keys=True))

        return DataPlacement(new_nodes, old_placement.partitions, new_locations)

    @staticmethod
    def compute_optimal_placement_complete(old_placement, new_nodes, access_statistics):
        # print('computing...........')
        # print(json.dumps(old_placement.locations, indent=4, sort_keys=True))
        # print(new_nodes)
        # print(json.dumps(access_statistics, indent=4, sort_keys=True))
        nodes_to_add = sorted(list(set(new_nodes + old_placement.nodes)))
        # print(nodes_to_add)
        partition_statistics = {}
        for node, partition_list in access_statistics.items():
            for partition, count in partition_list.items():
                if partition not in partition_statistics:
                    partition_statistics[partition] = 0
                partition_statistics[partition] += count

        # to make sure each partition is included
        for node, partition_list in old_placement.locations.items():
            for partition in partition_list:
                if partition not in partition_statistics:
                    partition_statistics[partition] = 0

        # print("statistics:", json.dumps(partition_statistics, indent=4, sort_keys=True))

        total_access = sum(partition_statistics.values())
        partition_statistics_percentage = {partition: count / total_access for partition, count in partition_statistics.items()}

        # print("statistics:", json.dumps(partition_statistics_percentage, indent=4, sort_keys=True))

        num_allocation = math.ceil(len(nodes_to_add) / len(old_placement.nodes) * sum([len(partition_list) for partition_list in old_placement.locations.values()]))
        # print("@@", num_allocation)

        # print('Target allocations:', num_allocation)
        max_partition_per_node = math.ceil(num_allocation / len(nodes_to_add))
        min_allocation_per_partition = 1
        max_allocation_per_partition = len(nodes_to_add)
        partition_allocation = {partition: round(num_allocation * percentage) for partition, percentage in partition_statistics_percentage.items()}

        # print("allocation:", json.dumps(partition_allocation, indent=4, sort_keys=True))

        partition_allocation_sorted = sorted(partition_allocation.items(), key=lambda x: partition_statistics_percentage[x[0]], reverse=True)
        # print('sorted allocation:', partition_allocation_sorted)

        partition_allocation_adjusted = copy.copy(partition_allocation_sorted)
        current_index = 0

        # Adjust to fit the mimium allocations > 1
        for i in range(len(partition_allocation_sorted)):
            partition, count = partition_allocation_sorted[i]
            if count <= 0:
                partition_allocation_adjusted[i] = (partition, min_allocation_per_partition)

        # Adjust to fit the maximum allocations
        for i in range(len(partition_allocation_sorted)):
            partition, count = partition_allocation_sorted[i]
            if count > max_allocation_per_partition:
                partition_allocation_adjusted[i] = (partition, max_allocation_per_partition)

        # print('1) adjusted allocation:')
        #for partition, count in partition_allocation_adjusted:
        #    print(count, partition)


        '''
        Allocation by access percentage may not fit to the total sum at all
        Case 1: < maximum total allocations -> increase from the very top one
        Case 2: > maximum total allocations -> decrease from the very down one
        '''
        satisfied = False
        while not satisfied:
            current_allocation = sum([count for _, count in partition_allocation_adjusted])
            # print("### current", current_allocation)
            if current_allocation == num_allocation:
                satisfied = True
            elif current_allocation < num_allocation:
                for i in range(len(partition_allocation_adjusted)):
                    if partition_allocation_adjusted[i][1] >= max_allocation_per_partition:
                        continue
                    else:
                        current_allocation = sum([count for _, count in partition_allocation_adjusted])
                        if current_allocation >= num_allocation:
                            break
                        else:
                            # print("(1)", partition_allocation_adjusted[i])
                            partition, count = partition_allocation_adjusted[i]
                            partition_allocation_adjusted[i] = (partition, count + 1)
                            # print("(2)", partition_allocation_adjusted[i])

            elif current_allocation > num_allocation:
                for i in reversed(range(len(partition_allocation_adjusted))):
                    if partition_allocation_adjusted[i][1] <= 1:
                        continue
                    else:
                        current_allocation = sum([count for _, count in partition_allocation_adjusted])
                        if current_allocation <= num_allocation:
                            break
                        else:
                            # print("(1)", partition_allocation_adjusted[i])
                            partition, count = partition_allocation_adjusted[i]
                            partition_allocation_adjusted[i] = (partition, count - 1)
                            # print("(2)", partition_allocation_adjusted[i])

        # print('2) adjusted allocation:')
        #for partition, count in partition_allocation_adjusted:
        #    print(count, partition)

        new_locations = {node: [] for node in nodes_to_add}
        current_node_index = 0
        for partition, count in partition_allocation_adjusted:
            for i in range(count):
                new_locations[nodes_to_add[current_node_index % len(nodes_to_add)]].append(partition)
                current_node_index += 1

        #new_locations.update(old_placement.locations)
        # print("optimal:", json.dumps(new_locations, indent=4, sort_keys=True))

        return DataPlacement(new_nodes, old_placement.partitions, new_locations)



