import os
import logging
import json

import executor

from elastic.benchmark import base
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.service import BenchmarkService


class RoxieBenchmark(base.Benchmark):

    def __init__(self, hpcc_cluster, benchmark_config, workload, output_dir="/tmp"):
        super(RoxieBenchmark, self).__init__(hpcc_cluster.get_roxie_cluster(), output_dir)
        self.hpcc_cluster = hpcc_cluster
        self.benchmark_config = benchmark_config
        self.benchmark_service = BenchmarkService(self.benchmark_config)
        self.workload = workload
        self.workload_id = None
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def pre_run(self):
        pass

    def post_run(self):
        self.logger.info("Post-benchmark")
        executor.execute("mkdir -p %s" % self.result_output_dir)
        report = self.benchmark_service.get_workload_report(self.workload_id)
        report_output_file = os.path.join(self.result_output_dir, "report.json")
        with open(report_output_file, 'w') as f:
            json.dump(report, f)

        statistics = self.benchmark_service.get_workload_statistics(self.workload_id)
        statistics_output_file = os.path.join(self.result_output_dir, "statistics.json")
        with open(statistics_output_file, 'w') as f:
            json.dump(statistics, f)

    def run_benchmark(self):
        self.logger.info("run benchmark")
        self.logger.info("output dir = {}".format(self.output_dir))
        self.workload_id = self.benchmark_service.submit_workload(self.workload)
        self.logger.info("workload id={}".format(self.workload_id))
        self.benchmark_service.wait_for_workload(self.workload_id)

    def clean_benchmark(self):
        pass

    def assert_service_status(self):
        if not self.benchmark_service.is_ready():
            raise Exception("benchmark service is not ready")