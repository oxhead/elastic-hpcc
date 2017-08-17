import os
import multiprocessing as mp
import itertools
import math
import json

import numpy as np
from scipy import stats
import statistics
import executor

#from mybenchmark.base import ExperimentConfig
from elastic.simulator import dp_simulation_new as dp_simulation
from elastic.benchmark.workload import SelectionModel


class SkewAnalyzer:
    @staticmethod
    def calculate_factor(access_list):
        '''
        from a paper
        '''
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

    @staticmethod
    def calculate_factor_scipy(access_list):
        '''
        from scipy
        '''
        return stats.skew(access_list)

    @staticmethod
    def calculate_factor_std(access_list):
        return np.std(access_list)

    @staticmethod
    def calculate_max_mean(access_list):
        return round(max(access_list) / np.mean(access_list), 4)

    @staticmethod
    def calculate_min_max(access_list):
        return round(min(access_list) / max(access_list), 4)

    def calculate_factor_myown(access_list):
        import numpy as np
        avg = np.mean(access_list)
        return sum([abs(n - avg) for n in access_list]) / len(access_list)

    def calculate_factor_myown_percentage(access_list):
        import numpy as np
        avg = np.mean(access_list)
        return sum([abs(n - avg) for n in access_list]) / avg

    def calculate_bottleneck(access_list):
        import numpy as np
        load_avg = np.mean(access_list)
        load_max = max(access_list)
        return load_max / load_avg


class ColorAnalyzer:
    @staticmethod
    def calculate_colors(color_list):
        return np.mean(color_list)

    @staticmethod
    def calculate_colors_std(color_list):
        return statistics.stdev(color_list)


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


def analyze_placement(M, N, k, scheme, af_list, workload_name='', show_output=False):

    dp, num_replicas_list = dp_simulation.run(M, N, k, scheme, af_list=af_list, workload_name=workload_name, show_output=show_output)

    load_list = [n.load for n in dp.nodes]
    color_list = [n.colors for n in dp.nodes]

    return {
        'max-mean': SkewAnalyzer.calculate_max_mean(load_list),
        'min-max': SkewAnalyzer.calculate_min_max(load_list),
        'skew': SkewAnalyzer.calculate_factor(load_list),
        'avg-colors': ColorAnalyzer.calculate_colors(color_list),
        'std-colors': ColorAnalyzer.calculate_colors_std(color_list),
        '5%-loads': np.percentile(load_list, 5),
        '25%-loads': np.percentile(load_list, 25),
        '50%-loads': np.percentile(load_list, 50),
        '75%-loads': np.percentile(load_list, 75),
        '95%-loads': np.percentile(load_list, 95),
        '5%-colors': np.percentile(color_list, 5),
        '25%-colors': np.percentile(color_list, 25),
        '50%-colors': np.percentile(color_list, 50),
        '75%-colors': np.percentile(color_list, 75),
        '95%-colors': np.percentile(color_list, 95),
    }, load_list, color_list


def analyze_placement_worker(*args, **kwargs):
    try:
        return analyze_placement(*args, **kwargs)
    except Exception as e:
        print("Unable to process: M={}, N={}, k={}, scheme={}".format(*args[:5]))
        raise e


def main(M, k_list, workload_size, workload_type_list, workload_skew_list, iterations):
    output_dir = os.path.join("results", "E43")

    # workload parameters
    workload_list = {
        "uniform-base": {"type": "uniform"},
        "beta-base": {"type": "beta", "alpha": 2, "beta": 2},
        "normal-base": {"type": "normal", "loc": 0, "scale": 1},
        "powerlaw-base": {"type": "powerlaw", "shape": 3},
        "gamma-base": {"type": "gamma", "shape": 5},
    }

    # generate workloads
    workload_records = {}
    for i in range(iterations):
        for workload_type in workload_type_list:
            num_partitions = M * k_list[-1]
            workload_name = "{}-base".format(workload_type)
            af_list = generate_workload(workload_size, num_partitions, workload_list[workload_name])
            #print(af_list)
            workload_records[(workload_name, i)] = af_list

    for workload_type, workload_skew in itertools.product(workload_type_list, workload_skew_list):
        workload_name = "{}-{}".format(workload_type, workload_skew)
        for i in range(iterations):
            af_list = workload_records[(workload_name, i)]
            print(workload_type, i)
            print("Max-Mean:", SkewAnalyzer.calculate_max_mean(af_list))
            print("Skew:", SkewAnalyzer.calculate_factor(af_list))


if __name__ == '__main__':
    workload_size = 300000
    workload_type_list = ['uniform', 'beta', 'normal', 'powerlaw', 'gamma']
    workload_skew_list = ['base']
    #k_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    k_list = [1024]
    iterations = 1
    print("M=4")
    main(4, k_list, workload_size, workload_type_list, workload_skew_list, iterations)
    print("M=64")
    main(64, k_list, workload_size, workload_type_list, workload_skew_list, iterations)
