import os
import json

from elastic.benchmark.workload import *


def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    workload_config = WorkloadConfig.parse_file(os.path.join(script_dir, 'workload.yaml'))
    workload = Workload.from_config(workload_config)
    workload_timeline = WorkloadExecutionTimeline.from_workload(workload)
    distribution_records = {}
    for t, workload_item_list in workload_timeline.next():
        for workload_item in workload_item_list:
            #print(workload_item.wid, workload_item.query_name)
            if workload_item.query_name not in distribution_records:
                distribution_records[workload_item.query_name] = 0
            distribution_records[workload_item.query_name] += 1

    for query_name in sorted(distribution_records.keys(), key=lambda x: int(x.split('_')[-1])):
        print(query_name, distribution_records[query_name])

    #print(workload_timeline.timeline)
    #print(json.dumps(workload_timeline.timeline, indent=4))
    pass

if __name__ == '__main__':
    main()
