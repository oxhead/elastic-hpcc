import logging
import sys

from elastic import init
from elastic.benchmark.zeromqimpl import *


def main(argv):
    # e.g. conf/benchmark.yaml
    benchmark_config_path = argv[0]
    # logging config
    logging_config_path = argv[1]
    log_dir = argv[2]
    init.setup_logging(default_level=logging.DEBUG, config_path=logging_config_path, log_dir=log_dir, component="controller")

    controller = BenchmarkController(benchmark_config_path)
    controller.start()


if __name__ == "__main__":
    main(sys.argv[1:])
