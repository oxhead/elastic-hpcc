import logging
import time
import random

from elastic.util import parallel
from elastic.benchmark.workload import Workload, WorkloadExecutionTimeline
from elastic.benchmark.zeromqimpl import BenchmarkCommander
from elastic.benchmark.zeromqimpl import BenchmarkConfig
from elastic.util import network as network_util


class BenchmarkService:

    @staticmethod
    def new(config_path):
        return BenchmarkService(BenchmarkConfig.parse_file(config_path))

    def __init__(self, config):
        self.config = config
        self.commander = BenchmarkCommander(self.config.get_controller(), self.config.lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT))
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
        self.sync_time = False

    def deploy_config(self):
        self.logger.info("deploy benchmark config")
        # TODO: lazy implementaiton, needs to improve here
        tmp_config_path = "/tmp/benchmark.yaml"
        config_path = "~/elastic-hpcc/conf/benchmark.yaml"

        self.config.to_file(tmp_config_path)

        deploy_set = set()
        deploy_set.add(self.config.get_controller())
        for driver_node in self.config.get_drivers():
            deploy_set.add(driver_node)
        with parallel.CommandAgent(concurrency=len(deploy_set)) as agent:
            for host in deploy_set:
                agent.submit_command('scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {} {}:{}'.format(tmp_config_path, host, config_path))

    def start(self):
        self.logger.info("start benchmark service")
        self.deploy_config()
        self.logger.info("sync time across servers")

        if self.sync_time:
            # https://svn.unity.ncsu.edu/svn/cls/tags/realmconfig/4.1.19/default-modules/ntp2.py
            ntp_servers = ['152.1.227.236', '152.1.227.237', '152.1.227.238']
            benchmark_nodes = [self.config.get_controller()] + self.config.get_drivers()
            with parallel.CommandAgent(concurrency=len(benchmark_nodes), show_result=False) as agent:
                agent.submit_remote_commands(benchmark_nodes, 'sudo ntpdate -u {}'.format(random.choice(ntp_servers)), silent=True)

        with parallel.CommandAgent(show_result=True) as agent:
            # TODO: a better way? Should not use fixed directory
            self.logger.info("start the controller node at {}".format(self.config.get_controller()))
            agent.submit_remote_command(self.config.get_controller(), 'bash ~/elastic-hpcc/script/start_controller.sh', silent=True)

            for driver_node in self.config.get_drivers():
                self.logger.info("start the driver node at {}".format(driver_node))
                agent.submit_remote_command(driver_node, 'bash ~/elastic-hpcc/script/start_driver.sh', silent=True)

    def stop(self):
        self.commander.stop()
        with parallel.CommandAgent(concurrency=len(self.config.get_drivers())) as agent:
            agent.submit_remote_commands(self.config.get_drivers(), "sudo pkill -9 python", check=False)

    def status(self):
        self.logger.info("check status of benchmark service")
        return self.commander.status()

    def is_ready(self):
        status_result = self.status()
        self.logger.debug("# of online drivers: {}".format(len(status_result)))
        self.logger.debug("# of drivers: {}".format(len(self.config.get_drivers())))
        return len(status_result) == len(self.config.get_drivers())

    def submit_workload(self, workload):
        if isinstance(workload, Workload):
            self.logger.debug("submit a workload object")
            workload_timeline = WorkloadExecutionTimeline.from_workload(workload)
        else:
            self.logger.debug("submit a workload timeline")
            workload_timeline = workload
        self.logger.debug(workload_timeline)
        return self.commander.workload_submit(workload_timeline)

    def run_workload(self, workload):
        workload_id = self.submit_workload(workload)
        self.wait_for_workload(workload_id)

    def wait_for_workload(self, workload_id):
        while True:
            if self.is_workload_completed(workload_id):
                break
            else:
                time.sleep(1)

    def get_workload_status(self, workload_id):
        return self.commander.workload_status(workload_id)

    def is_workload_completed(self, workload_id):
        return self.get_workload_status(workload_id)

    def get_workload_report(self, workload_id):
        return self.commander.workload_report(workload_id)

    def get_workload_statistics(self, workload_id):
        return self.commander.workload_statistics(workload_id)

    def get_workload_timeline_completion(self, workload_id):
        return self.commander.workload_timeline_completion(workload_id)

    def get_workload_timeline_failure(self, workload_id):
        return self.commander.workload_timeline_failure(workload_id)
