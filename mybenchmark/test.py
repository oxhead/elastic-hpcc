import os
import logging
import time
import itertools

from elastic import init
from elastic.benchmark.roxie import *
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.workload import *
from elastic.hpcc.base import *
from elastic.util import helper


arrival_types = ["poisson", "constant"]
distribution_types = ["fixed", "uniform", "gaussian"]

def main():
    init.setup_logging(config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    hpcc_cluster = HPCCCluster.parse_config("/etc/HPCCSystems/source/hpcc_t5_r5_cyclic.xml")
    benchmark_config = BenchmarkConfig.parse_file("/home/chsu6/elastic-hpcc/conf/benchmark.yaml")


    workloads = itertools.product(arrival_types, distribution_types)

    for arrival_type, distribution_type in workloads:
        workload_config = "/home/chsu6/elastic-hpcc/conf/workload_{}_{}.yaml".format(arrival_type, distribution_type)
        workload = Workload.from_config(workload_config)
        output_dir = os.path.join("benchmark_results", "5roxie_{}_{}_100queries_120sec".format(arrival_type, distribution_type))
        bm = RoxieBenchmark(hpcc_cluster, benchmark_config, workload, output_dir=output_dir)
        bm.run()
        time.sleep(10)
    
if __name__ == "__main__":
    main()
