import sys
import logging

from elastic import init
from elastic.benchmark.impl.zeromqimpl import BenchmarkDriver

if __name__ == '__main__':
    driver_id = int(sys.argv[1])
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="driver_{}".format(driver_id))

    config_path = '/home/chsu6/elastic-hpcc/conf/benchmark.yaml.test'
    driver = BenchmarkDriver(config_path, driver_id)
    driver.start()
