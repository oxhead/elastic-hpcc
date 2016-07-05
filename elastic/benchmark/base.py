import bisect
import copy
from enum import Enum
import os
import time
import traceback
import datetime
import logging
import random
import traceback

import yaml

from elastic import base
from elastic.util.monitoringtool import Monitor
from elastic.util import parallel


class Benchmark:
    def __init__(self, cluster, output_dir, retries=1, **kwargs):
        self.cluster = cluster
        self.output_dir = output_dir
        self.retries = retries
        self.result_output_dir = os.path.join(self.output_dir, "result")
        self.monitoring_output_dir = os.path.join(self.output_dir, "monitoring")
        self.config_output_dir = os.path.join(self.output_dir, "config")
        self.kwargs = kwargs
        self.monitor = Monitor(self.cluster, self.monitoring_output_dir)
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
        self.sync_time = False

    def pre_run(self):
        self.logger.info("sync time across servers")
        # only needs to sync time on VCL so far
        if self.sync_time:
            # https://svn.unity.ncsu.edu/svn/cls/tags/realmconfig/4.1.19/default-modules/ntp2.py
            ntp_servers = ['152.1.227.236', '152.1.227.237', '152.1.227.238']
            with parallel.CommandAgent(concurrency=len(self.cluster.get_nodes()), show_result=False) as agent:
                agent.submit_remote_commands(self.cluster.get_nodes(), 'sudo ntpdate -u {}'.format(random.choice(ntp_servers)), silent=True)

    def post_run(self):
        raise NotImplementedError()

    def run_benchmark(self):
        raise NotImplementedError()

    def clean_benchmark(self):
        raise NotImplementedError()

    def assert_service_status(self):
        raise NotImplementedError()

    def run(self):
        for i in range(1, self.retries + 1):
            self.logger.info("Run the benchmark the %s time" % i)
            try:
                self.benchmark()
                return True
            except KeyboardInterrupt:
                raise Exception("Interrupted by the user")
            except SystemExit:
                traceback.format_exc()
                raise Exception("Unknown system exception")
            except:
                self.logger.exception("Failed to run benchmark")
            finally:
                self.clean_benchmark()
           
        raise Exception("Unable to finish benchmark due to exceeding maximum %s retries" % self.retries)
            
    def benchmark(self):
        self.time_start = int(time.time())
        self.pre_run()
        self.assert_service_status()
        self.monitor.start()
        self.run_benchmark()
        self.monitor.stop()
        self.post_run()
        self.time_end = int(time.time())

        self.logger.info("start time: %s" % datetime.datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("end time: %s" % datetime.datetime.fromtimestamp(self.time_end).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("elapsed time: %s sec" % (self.time_end - self.time_start))
