import os
import sys
import json

from mybenchmark.base import config as myconfig

def main(argv):
    print(argv)

    items = [
        os.path.join(myconfig['result_dir'], 'test', 'test1', 'single', 'result'),
        os.path.join(myconfig['result_dir'], 'test', 'test1', 'uniform', 'result')
    ]

    for item in items:
        records = json.load(open(os.path.join(item, "statistics.json")))
        time_list = [info['elapsedTime'] for (wid, info) in records.items()]
        print(item)
        print(sum(time_list)/len(time_list))


if __name__ == "__main__":
    main(sys.argv[1:])