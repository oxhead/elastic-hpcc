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


def main():
    init.setup_logging(config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    hpcc_cluster = HPCCCluster.parse_config("/etc/HPCCSystems/source/hpcc_t5_r5_cyclic.xml")
    benchmark_config = BenchmarkConfig.parse_file("/home/chsu6/elastic-hpcc/conf/benchmark.yaml")

    workload_config = "/home/chsu6/elastic-hpcc/conf/workload_simple.yaml"
    workload = Workload.from_config(workload_config)
    output_dir = os.path.join("benchmark_results", "/tmp/test_workload")
    bm = RoxieBenchmark(hpcc_cluster, benchmark_config, workload, output_dir=output_dir)
    bm.run()
    
if __name__ == "__main__":
    main()
