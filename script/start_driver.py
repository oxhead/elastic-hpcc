from elastic import init
from elastic.benchmark.zeromqimpl import *


def main(argv):
    # e.g. conf/benchmark.yaml
    benchmark_config_path = argv[0]
    # logging config
    logging_config_path = argv[1]
    log_dir = argv[2]
    init.setup_logging(config_path=logging_config_path, log_dir=log_dir, component="driver")

    driver = BenchmarkDriver(benchmark_config_path)
    driver.start()


if __name__ == "__main__":
    main(sys.argv[1:])
