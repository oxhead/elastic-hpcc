import os
import logging
import time
import itertools
import copy
import json

from elastic import init
from elastic.benchmark.roxie import *
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.workload import Workload, WorkloadConfig, WorkloadExecutionTimeline

from elastic.hpcc.base import *
from elastic.util import helper


#arrival_types = ["poisson", "constant"]
arrival_types = ["constant"]
#num_queries_list = [1600]
num_queries_list = [100]
period_list = [120]
distribution_types = [
    {"type": "pareto", "alpha": 3},
    #{"type": "uniform"},
    #{"type": "gaussian", "mu": 1, "sigma": 0.2}
]
applications = ['bm1']


def run(run_output_dir, workload_file):
    hpcc_cluster = HPCCCluster.parse_config("/home/ubuntu/elastic-hpcc/template/elastic_4roxie.xml")
    benchmark_config = BenchmarkConfig.parse_file("/home/ubuntu/elastic-hpcc/conf/4driver.yaml")
    #benchmark_config = BenchmarkConfig.parse_file("/home/ubuntu/elastic-hpcc/conf/1driver.yaml")

    script_dir = os.path.dirname(os.path.realpath(__file__))

    workload_config_template = WorkloadConfig.parse_file(os.path.join(script_dir, workload_file))
    application_db = workload_config_template.lookup_config('workload.applications')

    for arrival_type in arrival_types:
        for num_queries in num_queries_list:
            for period in period_list:
                workload_config = copy.deepcopy(workload_config_template)
                workload_config.set_config('workload.type', arrival_type)
                workload_config.set_config('workload.num_queries', num_queries)
                workload_config.set_config('workload.period', period)
                workload = Workload.from_config(workload_config)
                workload_timeline = WorkloadExecutionTimeline.from_workload(workload)
                for app_name in applications:
                    for distribution_type in distribution_types:
                        per_workload_config = copy.deepcopy(workload_config)
                        per_workload_config.set_config('workload.distribution', distribution_type)
                        per_workload_config.set_config('workload.applications', {app_name: application_db[app_name]})
                        per_workload = Workload.from_config(per_workload_config)
                        per_workload_timeline = WorkloadExecutionTimeline.from_timeline(workload_timeline, per_workload)

                        output_dir = os.path.join("results", run_output_dir,
                                                  "4roxie_{}_{}_{}_{}queries_{}sec".format(arrival_type,
                                                                                           distribution_type['type'],
                                                                                           app_name, num_queries,
                                                                                           period))
                        if os.path.exists(output_dir):
                            continue

                        bm = RoxieBenchmark(hpcc_cluster, benchmark_config, per_workload_timeline,
                                            output_dir=output_dir)
                        time.sleep(60)
                        bm.run()


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")

    workload_files = ["workload_template3.yaml"]

    for i in range(0, 1):
        workload_file = workload_files[i]

        output_dir = "E2/layout_perfect/single"
        #output_dir = "layout_1node_400MB_2"
        # output_dir = "layout_perfect_400MB_pareto_2"

        #output_dir = "layout_2node_400MB_pareto"
        #output_dir = "layout_1node_400MB_pareto"
        run(output_dir, workload_file)


if __name__ == "__main__":
    main()
