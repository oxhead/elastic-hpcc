import logging

from elastic import init
from elastic.benchmark.impl.zeromqimpl import BenchmarkController

if __name__ == '__main__':
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="controller")

    config_path = '/home/chsu6/elastic-hpcc/conf/benchmark.yaml.test'
    controller = BenchmarkController(config_path)
    controller.start()
