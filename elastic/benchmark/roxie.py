import os
import logging
import json

import executor

from elastic.benchmark import base
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.service import BenchmarkService
from elastic.hpcc import roxie
from elastic.util import parallel


class RoxieBenchmark(base.Benchmark):

    def __init__(self, hpcc_cluster, benchmark_config, workload_timeline, output_dir="/tmp"):
        super(RoxieBenchmark, self).__init__(hpcc_cluster.get_roxie_cluster(), output_dir)
        self.hpcc_cluster = hpcc_cluster
        self.benchmark_config = benchmark_config
        self.benchmark_service = BenchmarkService(self.benchmark_config)
        self.workload_timeline = workload_timeline
        self.workload_id = None
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def pre_run(self):
        self.logger.info("resetting Roxie metrics")
        for node in self.cluster.get_nodes():
            roxie.reset_metrics(node)

    def post_run(self):
        self.logger.info("Post-benchmark")
        executor.execute("mkdir -p %s" % self.result_output_dir)
        executor.execute("mkdir -p %s" % self.config_output_dir)

        self.logger.info("exporting workload report")
        report = self.benchmark_service.get_workload_report(self.workload_id)
        report_output_file = os.path.join(self.result_output_dir, "report.json")
        with open(report_output_file, 'w') as f:
            json.dump(report, f)

        self.logger.info("exporting detailed statistics")
        statistics = self.benchmark_service.get_workload_statistics(self.workload_id)
        statistics_output_file = os.path.join(self.result_output_dir, "statistics.json")
        with open(statistics_output_file, 'w') as f:
            json.dump(statistics, f)

        self.logger.info("exporting timeline recording")
        workload_output_file = os.path.join(self.config_output_dir, "workload_timline.pickle")
        self.workload_timeline.to_pickle(workload_output_file)

        self.logger.info("exporting Roxie metrics")
        for node in self.cluster.get_nodes():
            metrics = roxie.get_metrics(node)
            metrics_output_file = os.path.join(self.monitoring_output_dir, "{}.json".format(node.get_ip()))
            with open(metrics_output_file, 'w') as f:
                json.dump(metrics, f)

    def run_benchmark(self):
        self.logger.info("run benchmark")
        self.logger.info("output dir = {}".format(self.output_dir))
        self.workload_id = self.benchmark_service.submit_workload(self.workload_timeline)
        self.logger.info("workload id={}".format(self.workload_id))
        self.benchmark_service.wait_for_workload(self.workload_id)

    def clean_benchmark(self):
        pass

    def assert_service_status(self):
        if not self.benchmark_service.is_ready():
            raise Exception("benchmark service is not ready")