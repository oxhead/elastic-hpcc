import sys

from elastic.benchmark.zeromqimpl import *

def main(argv):
    config_path = argv[0]
    controller = BenchmarkController(config_path)
    controller.start()


if __name__ == "__main__":
    main(sys.argv[1:])
