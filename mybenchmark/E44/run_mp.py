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
        import time
        time_start = time.time()
        result = analyze_placement(*args, **kwargs)
        time_end = time.time()
        print("Total Time:", (time_end - time_start))
        return result
    except Exception as e:
        print("Unable to process: M={}, N={}, k={}, scheme={}".format(*args[:5]))
        raise e


def main(M, N_list, k_list, scheme_list, workload_size, workload_type_list, workload_skew_list, metric_list, iterations):
    pool = mp.Pool(processes=mp.cpu_count())

    output_dir = os.path.join("results", "E42")
    executor.execute("mkdir -p %s" % output_dir)

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

                    # check existence
                    record_file = os.path.join(output_dir, "{}_{}_M{}_N{}_k{}_{}.loads".format(workload_type, scheme, M, N, k, i + 1))
                    if os.path.exists(record_file):
                        simulation_run_list.append(None)
                        continue

                    af_list = workload_records[(workload_name, i)]
                    k_largest = k_list[-1]
                    aggregate_ratio = int(k_largest / k)
                    num_partitions = M * k
                    partition_workload = [sum(af_list[i * aggregate_ratio:(i + 1) * aggregate_ratio]) for i in range(num_partitions)]
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
                simulation_data[wid][N][k] = [r.get()[0] if r is not None else None for r in simulation_results[wid][N][k]]

                raw_data_list = [r.get()[1:] if r is not None else (None, None) for r in simulation_results[wid][N][k]]
                for i in range(len(raw_data_list)):
                    load_list, color_list = raw_data_list[i]
                    if load_list is None:
                        continue
                    load_output_path = os.path.join(output_dir, "{}_{}_M{}_N{}_k{}_{}.loads".format(workload_type, scheme, M, N, k, i+1))
                    color_output_path = os.path.join(output_dir, "{}_{}_M{}_N{}_k{}_{}.colors".format(workload_type, scheme, M, N, k, i+1))
                    with open(load_output_path, 'w') as f:
                        for load in load_list:
                            f.write("{}\n".format(load))
                    with open(color_output_path, 'w') as f:
                        for color in color_list:
                            f.write("{}\n".format(color))

                # don't need to load data anymore
                # metric_records = {}
                # for metric in simulation_data[wid][N][k][0].keys():
                #    metric_record = [record[metric] for record in simulation_data[wid][N][k]]
                #    metric_records[metric] = statistics.mean(metric_record)

                # simulation_output[N][k] = metric_records

        # for metric in metric_list:
        #    output_path = os.path.join(output_dir, "M{}_{}_{}_{}.json".format(M, workload_type, scheme, metric))

        #    records_output = {key_N: {key_k: record[metric] for key_k, record in simulation_output[key_N].items()} for key_N in simulation_output.keys()}

        #    with open(output_path, 'w') as f:
        #        json.dump(records_output, f, indent=True)


if __name__ == '__main__':
    #M_list = [4, 16, 64]
    M_list = [64]
    #R_list = [1, 1.5, 2, 2.5, 3, 3.5, 4, ]
    #R_list = [1, 1.5, 2, 2.5, 3, 3.5, 4, 8, 16, 32, 64]
    #R_list = [1, 2, 4, 8, 16, 32, 64]
    R_list = [4]
    #R_list = [4]
    #k_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    #k_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    k_list = [64]
    #scheme_list = ['mc', 'ml', 'mlb']
    scheme_list = ['ml']
    workload_size = 300000
    #workload_type_list = ['uniform', 'beta', 'normal', 'powerlaw', 'gamma']
    workload_type_list = ['powerlaw']
    workload_skew_list = ['base']
    metric_list = ['max-mean', 'min-max', 'skew', 'avg-colors', 'std-colors', '5%-loads', '25%-loads', '50%-loads', '75%-loads', '95%-loads']
    iterations = 1
    for M in M_list:
        N_list = [int(M * r) for r in R_list]
        main(M, N_list, k_list, scheme_list, workload_size, workload_type_list, workload_skew_list, metric_list, iterations)
