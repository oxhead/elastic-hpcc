import bisect
import copy
from enum import Enum
import os
import time
import traceback
import datetime
import logging
import random

import yaml

from elastic import base
from elastic.util.monitoringtool import Monitor


class Benchmark:
    def __init__(self, cluster, output_dir, retries=3, **kwargs):
        self.cluster = cluster
        self.output_dir = output_dir
        self.retries = retries
        self.result_output_dir = os.path.join(self.output_dir, "result")
        self.monitoring_output_dir = os.path.join(self.output_dir, "monitoring")
        self.config_output_dir = os.path.join(self.output_dir, "config")
        self.kwargs = kwargs
        self.monitor = Monitor(self.cluster, self.monitoring_output_dir)

    def pre_run(self):
        raise NotImplementedError()

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

        self.logger.info("[Time] Start  : %s" % datetime.datetime.fromtimestamp(self.time_start).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] End    : %s" % datetime.datetime.fromtimestamp(self.time_end).strftime('%Y-%m-%d %H:%M:%S'))
        self.logger.info("[Time] Elapsed: %s sec" % (self.time_end - self.time_start))

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)
