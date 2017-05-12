import json
import sys
import glob
import os


def main(file_path):
    with open(file_path, 'r') as f:
        dp = json.load(f)
    #print(json.dumps(dp, indent=4, sort_keys=True))

    dp_colors = {}
    for host, partition_list in dp.items():
        dp_colors[host] = len(set(partition_list))
    #print(json.dumps(dp_colors, indent=4, sort_keys=True))
    num_nodes = len(dp_colors)
    sum_colors = sum(dp_colors.values())
    min_colors = min(dp_colors.values())
    max_colors = max(dp_colors.values())
    avg_colors = sum_colors / num_nodes
    print("avg={}, min={}, max={}".format(avg_colors, min_colors, max_colors))


if __name__ == "__main__":
    file_path = sys.argv[1]
    main(file_path)






