import itertools
import copy
from collections import defaultdict
import json
import math

import executor
from scipy import stats

from mybenchmark.base import *
from elastic.simulator import dp_simulation
from elastic.benchmark.workload import SelectionModel


def calculate_skew_factor(access_list):
    num_partitions = len(access_list)
    total = sum(access_list)
    # print("total", total)
    best = 1 / num_partitions
    # print("best", best)
    skew = 0
    for index in range(num_partitions):
        ratio = access_list[index] / total
        # print(index, ratio)
        if ratio < best:
            ratio = best + ((1 - ratio / best) * (1 - best))
        skew += math.log(ratio / best, 10)
    return skew / (math.log(1 / best, 10) * num_partitions)

def calculate_skew_factor_scipy(access_list):
    return stats.skew(access_list)

def calculate_skew_factor_std(access_list):
    import numpy as np
    return np.std(access_list)

def calculate_skew_factor_myown(access_list):
    import numpy as np
    avg = np.mean(access_list)
    return sum([abs(n-avg) for n in access_list]) / len(access_list)

def calculate_skew_factor_myown_percentage(access_list):
    import numpy as np
    avg = np.mean(access_list)
    return sum([abs(n-avg) for n in access_list]) / avg

def calculate_bottleneck(access_list):
    import numpy as np
    load_avg = np.mean(access_list)
    load_max = max(access_list)
    return load_max / load_avg

def calculate_load(dp_records, num_replicas_list, af_list):
    partition_load_records = dp_simulation.calculate_partition_loads(num_replicas_list, af_list)
    node_load_records = defaultdict(lambda: 0)
    for node_id, partition_list in dp_records.items():
        for partition_id in partition_list:
            node_load_records[node_id] += partition_load_records[partition_id]
    return node_load_records


def generate_workload(workload_size, num_partitions, workload):
    workload_config = {
        'kind': 'nature',
        'num_chunks': num_partitions,
    }
    workload_config.update(workload)
    objects = [n for n in range(1, num_partitions+1)]
    w = SelectionModel.new(workload_config, objects)
    record = {}
    for n in objects:
        record[n] = 0
    for i in range(workload_size):
        n = w.select()
        record[n] += 1
    access_list = []
    for n in objects:
        access_list.append(record[n])
    #print(json.dumps(record, indent=4, sort_keys=True))
    return access_list

def test1():
    workload_list = {
        "uniform-base": {"type": "uniform"},
        #"beta-least": {"type": "beta", "alpha": 1, "beta": 1},
        #"beta-less": {"type": "beta", "alpha": 1.5, "beta": 1.5},
        "beta-base": {"type": "beta", "alpha": 2, "beta": 2},
        #"beta-more": {"type": "beta", "alpha": 4, "beta": 4},
        #"beta-most": {"type": "beta", "alpha": 5, "beta": 5},
        #"normal-base": {"type": "normal", "loc": 0, "scale": 1},
        #"powerlaw-least": {"type": "powerlaw", "shape": 2},
        #"powerlaw-less": {"type": "powerlaw", "shape": 2.5},
        "powerlaw-base": {"type": "powerlaw", "shape": 3},
        #"powerlaw-more": {"type": "powerlaw", "shape": 4},
        #"powerlaw-most": {"type": "powerlaw", "shape": 5},
        #"gamma-least": {"type": "gamma", "shape": 7},
        #"gamma-less": {"type": "gamma", "shape": 6},
        #"gamma-base": {"type": "gamma", "shape": 5},
        "gamma-more": {"type": "gamma", "shape": 4},
        "gamma-most": {"type": "gamma", "shape": 3},
    }
    workload_type = 'gamma'
    generate_workload(16, workload_list[workload_type])


def analyze_skew(M, N, k, scheme, af_list):

    dp_records, num_replicas_list = dp_simulation.run(M, N, k, scheme, af_list=af_list, show_output=False)

    load_records = calculate_load(dp_records, num_replicas_list, af_list)
    print('#######################')
    print(json.dumps(load_records, indent=4))
    print('#######################')
    #print(load_records)
    #print("Skew:", calculate_skew_factor(list(load_records.values())))
    #skew_score = calculate_skew_factor(list(load_records.values()))
    #skew_score_scipy = calculate_skew_factor_scipy(list(load_records.values()))
    #print("@", skew_score, skew_score_scipy)
    #print("#", calculate_skew_factor(af_list), calculate_skew_factor_scipy(af_list))
    #return skew_score_scipy
    import numpy as np
    #print("@", sum(load_records.values()))
    #print("#", len(load_records))
    #return np.std(list(load_records.values())) / sum(load_records.values())
    #return np.std(list(load_records.values()))
    #return calculate_skew_factor_myown_percentage(list(load_records.values()))
    return calculate_skew_factor(list(load_records.values()))
    #return calculate_bottleneck(list(load_records.values()))

def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="simulation")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    workload_list = {
        "uniform-base": {"type": "uniform"},
        # "beta-least": {"type": "beta", "alpha": 1, "beta": 1},
        # "beta-less": {"type": "beta", "alpha": 1.5, "beta": 1.5},
        "beta-base": {"type": "beta", "alpha": 2, "beta": 2},
        # "beta-more": {"type": "beta", "alpha": 4, "beta": 4},
        # "beta-most": {"type": "beta", "alpha": 5, "beta": 5},
        "normal-base": {"type": "normal", "loc": 0, "scale": 1},
        # "powerlaw-least": {"type": "powerlaw", "shape": 2},
        # "powerlaw-less": {"type": "powerlaw", "shape": 2.5},
        "powerlaw-base": {"type": "powerlaw", "shape": 3},
        # "powerlaw-more": {"type": "powerlaw", "shape": 4},
        # "powerlaw-most": {"type": "powerlaw", "shape": 5},
        # "gamma-least": {"type": "gamma", "shape": 7},
        # "gamma-less": {"type": "gamma", "shape": 6},
        "gamma-base": {"type": "gamma", "shape": 5},
        # "gamma-more": {"type": "gamma", "shape": 4},
        # "gamma-most": {"type": "gamma", "shape": 3},
    }

    iterations = 10

    M = 4
    N = 8
    scheme_list = ['rainbow', 'monochromatic']
    #scheme_list = ['rainbow']
    #scheme_list = ['monochromatic']
    workload_type_list = ['uniform', 'beta', 'normal', 'powerlaw', 'gamma']
    #workload_type_list = ['normal']
    #workload_type_list = ['powerlaw']
    #workload_skew_list = ['least', 'less', 'base', 'more', 'most']
    workload_skew_list = ['base']

    workload_size = 30000
    k_list = [1, 4, 8, 16, 32, 64, 128]
    #k_list = [1, 4, 8, 16]
    #k_list = [4, 8]
    #k_list = [8, 16]
    #k_list = [1, 4]



    # start multi-iteration simulation


    # reuse workload
    workload_records = {}
    for i in range(iterations):
        for workload_type in workload_type_list:
            num_partitions = M * k_list[-1]
            workload_name = "{}-base".format(workload_type)
            af_list = generate_workload(workload_size, num_partitions, workload_list[workload_name])
            workload_records[(workload_name, i)] = af_list

    for scheme in scheme_list:
        skew_records = {}
        for k in k_list:
            skew_records[k] = {}
            for workload_type, workload_skew in itertools.product(workload_type_list, workload_skew_list):
                skew_score_list = []
                for i in range(iterations):
                    workload_name = "{}-{}".format(workload_type, workload_skew)
                    af_list = workload_records[(workload_name, i)]
                    k_largest = k_list[-1]
                    aggregate_ratio = int(k_largest / k)
                    num_partitions = M * k
                    partition_workload = [sum(af_list[i*aggregate_ratio:(i+1)*aggregate_ratio])for i in range(num_partitions)]

                    skew_score = analyze_skew(M, N, k, scheme, partition_workload)
                    skew_score_list.append(skew_score)
                skew_records[k][workload_name] = sum(skew_score_list) / len(skew_score_list)

        print('=================')
        print(json.dumps(skew_records, indent=True, sort_keys=True))

        default_setting = ExperimentConfig.new()
        default_setting.set_config('experiment.id', 'E29')
        output_dir = os.path.join(
            default_setting['experiment.result_dir'],
            default_setting['experiment.id']
        )
        output_path = os.path.join(output_dir, "M{}_N{}_k{}_s{}_{}.json".format(M, N, k_list[-1], workload_size, scheme))
        #output_path = os.path.join(output_dir, "{}.json".format(scheme))

        executor.execute("mkdir -p %s" % output_dir)
        with open(output_path, 'w') as f:
            json.dump(skew_records, f, indent=True)


if __name__ == "__main__":
    main()
    #test1()
