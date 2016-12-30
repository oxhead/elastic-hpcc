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

    def __init__(self, hpcc_cluster, benchmark_config, workload_timeline, output_dir="/tmp", routing_table={}):
        super(RoxieBenchmark, self).__init__(hpcc_cluster.get_roxie_cluster(), output_dir)
        self.hpcc_cluster = hpcc_cluster
        self.benchmark_config = benchmark_config
        self.routing_table = routing_table
        self.benchmark_service = BenchmarkService(self.benchmark_config)
        self.workload_timeline = workload_timeline
        self.workload_id = None
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def pre_run(self):
        super(RoxieBenchmark, self).pre_run()
        self.logger.info("resetting Roxie metrics")
        with parallel.ThreadAgent() as agent:
            for node in self.cluster.get_nodes():
                roxie.reset_metrics(node)
                agent.submit(node.get_ip(), roxie.reset_metrics, node)
        self.benchmark_service.stop()
        self.benchmark_service.start()
        self.benchmark_service.upload_routing_table(self.routing_table)

    def post_run(self):
        self.logger.info("Post-benchmark")
        executor.execute("mkdir -p %s" % self.result_output_dir)
        executor.execute("mkdir -p %s" % self.config_output_dir)

        self.logger.info("exporting workload report")
        report = self.benchmark_service.get_workload_report(self.workload_id)
        report_output_file = os.path.join(self.result_output_dir, "report.json")
        with open(report_output_file, 'w') as f:
            json.dump(report, f, indent=4, sort_keys=True)

        self.logger.info("exporting detailed statistics")
        statistics = self.benchmark_service.get_workload_statistics(self.workload_id)
        statistics_json_output_file = os.path.join(self.result_output_dir, "statistics.json")
        with open(statistics_json_output_file, 'w') as f:
            json.dump(statistics, f, indent=4, sort_keys=True)
        statistics_csv_output_file = os.path.join(self.result_output_dir, "statistics.csv")
        with open(statistics_csv_output_file, 'w') as f:
            f.write("wid,success,status,size,queueTimestamp,startTimestamp,finishTimestamp,queryTime,totalTime\n")
            for query_id, query_statistics  in statistics.items():
                f.write("{},{},{},{},{},{},{},{},{}\n".format(
                    query_id,
                    1 if query_statistics['success'] else 0, query_statistics['status'], query_statistics['size'],
                    query_statistics['queueTimestamp'],
                    query_statistics['startTimestamp'],
                    query_statistics['finishTimestamp'],
                    float(query_statistics['finishTimestamp']) - float(query_statistics['startTimestamp']),
                    float(query_statistics['finishTimestamp']) - float(query_statistics['queueTimestamp']))
                )

        self.logger.info("exporting query completion timeline")
        completion_timeline = self.benchmark_service.get_workload_timeline_completion(self.workload_id)
        completion_timeline_file = os.path.join(self.result_output_dir, "completion_timeline.json")
        with open(completion_timeline_file, 'w') as f:
            json.dump(completion_timeline, f, indent=4, sort_keys=True)

        self.logger.info("exporting query failulre timeline")
        failure_timeline = self.benchmark_service.get_workload_timeline_failure(self.workload_id)
        failure_timeline_file = os.path.join(self.result_output_dir, "failure_timeline.json")
        with open(failure_timeline_file, 'w') as f:
            json.dump(failure_timeline, f, indent=4, sort_keys=True)

        self.logger.info("exporting benchmark config")
        benchmark_config_output = os.path.join(self.config_output_dir, "benchmark.yaml")
        self.benchmark_service.export_config(benchmark_config_output)

        self.logger.info("stopping benchmark service")
        self.benchmark_service.stop()

        self.logger.info("exporting timeline recording")
        workload_output_file = os.path.join(self.config_output_dir, "workload_timline.pickle")
        self.workload_timeline.to_pickle(workload_output_file)

        self.logger.info("exporting Roxie metrics")
        for node in self.cluster.get_nodes():
            metrics = roxie.get_metrics(node)
            metrics_output_file = os.path.join(self.monitoring_output_dir, "roxie_{}.json".format(node.get_ip()))
            with open(metrics_output_file, 'w') as f:
                json.dump(metrics, f, indent=4, sort_keys=True)

        self.logger.info("exporting Roxie load distribution")
        workload_distribution = {}
        for node in self.cluster.get_nodes():
            workload_distribution[node.get_ip()] = roxie.get_workload_distribution(node)
        workload_distribution_output_file = os.path.join(self.result_output_dir, "workload_distribution.json")
        with open(workload_distribution_output_file, 'w') as f:
            json.dump(workload_distribution, f, indent=4, sort_keys=True)

        self.logger.info("exporting Roxie partition access distribution")
        access_distribution = roxie.get_part_access_statistics(self.cluster.get_nodes())
        access_distribution_output_file = os.path.join(self.result_output_dir, "access_distribution.json")
        with open(access_distribution_output_file, 'w') as f:
            json.dump(access_distribution, f, indent=4, sort_keys=True)

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