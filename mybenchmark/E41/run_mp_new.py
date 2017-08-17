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
        'std-colors': ColorAnalyzer.calculate_colors_std(color_list)
    }


def analyze_placement_worker(*args, **kwargs):
    return analyze_placement(*args, **kwargs)


def main():
    pool = mp.Pool(processes=mp.cpu_count())

    #default_setting = ExperimentConfig.new()
    #default_setting.set_config('experiment.id', 'E41')
    output_dir = os.path.join("results", "E41")
    executor.execute("mkdir -p %s" % output_dir)

    workload_list = {
        "uniform-base": {"type": "uniform"},
        "beta-base": {"type": "beta", "alpha": 2, "beta": 2},
        "normal-base": {"type": "normal", "loc": 0, "scale": 1},
        "powerlaw-base": {"type": "powerlaw", "shape": 3},
        "gamma-base": {"type": "gamma", "shape": 5},
    }

    iterations = 3

    M = 8
    # N_list = [8, 12, 16, 32, 64, 128, 256]
    N_list = [8]


    # scheme_list = ['mc', 'ml', 'mlb']
    # scheme_list = ['ml', 'mlb']
    scheme_list = ['ml']
    # workload_type_list = ['uniform', 'beta', 'normal', 'powerlaw', 'gamma']
    workload_type_list = ['powerlaw']
    workload_skew_list = ['base']
    metric_list = ['max-mean', 'min-max', 'skew', 'avg-colors', 'std-colors']

    workload_size = 30000
    k_list = [1, 2, 3, 4, 8, 16, 32, 64, 128]
    #k_list = [8]

    # generate workloads
    workload_records = {}
    for i in range(iterations):
        for workload_type in workload_type_list:
            num_partitions = M * k_list[-1]
            workload_name = "{}-base".format(workload_type)
            af_list = generate_workload(workload_size, num_partitions, workload_list[workload_name])
            workload_records[(workload_name, i)] = af_list

    # put every simulation jobs into queue
    simulation_results = {}
    for workload_type, workload_skew, scheme in itertools.product(workload_type_list, workload_skew_list, scheme_list):
        wid = (workload_type, workload_skew, scheme)
        simulation_results[wid] = {}
        for N in N_list:
            simulation_results[wid][N] = {}
            for k in k_list:
                simulation_run_list = []
                workload_name = "{}-{}".format(workload_type, workload_skew)
                for i in range(iterations):
                    af_list = workload_records[(workload_name, i)]
                    k_largest = k_list[-1]
                    aggregate_ratio = int(k_largest / k)
                    num_partitions = M * k
                    partition_workload = [sum(af_list[i * aggregate_ratio:(i + 1) * aggregate_ratio]) for i in range(num_partitions)]
                    #skew_score = analyze_placement(M, N, k, scheme, partition_workload, workload_name=workload_name, show_output=False)
                    simulation_run_list.append(
                        pool.apply_async(
                            analyze_placement_worker,
                            (M, N, k, scheme, partition_workload, workload_name, False)
                        )
                    )
                simulation_results[wid][N][k] = simulation_run_list

    pool.close()
    pool.join()

    # get all the results
    simulation_data = {}
    for workload_type, workload_skew, scheme in itertools.product(workload_type_list, workload_skew_list, scheme_list):
        wid = (workload_type, workload_skew, scheme)
        simulation_data[wid] = {}
        simulation_output = {}
        for N in N_list:
            simulation_data[wid][N] = {}
            simulation_output[N] = {}
            for k in k_list:
                simulation_data[wid][N][k] = []
                simulation_data[wid][N][k] = [r.get() for r in simulation_results[wid][N][k]]

                metric_records = {}
                for metric in simulation_data[wid][N][k][0].keys():
                    metric_record = [record[metric] for record in simulation_data[wid][N][k]]
                    metric_records[metric] = statistics.mean(metric_record)

                simulation_output[N][k] = metric_records

        for metric in metric_list:
            output_path = os.path.join(output_dir, "M{}_{}_{}_{}.json".format(M, workload_type, scheme, metric))

            records_output = {key_N: {key_k: record[metric] for key_k, record in simulation_output[key_N].items()} for key_N in simulation_output.keys()}

            with open(output_path, 'w') as f:
                json.dump(records_output, f, indent=True)


if __name__ == '__main__':
    main()
