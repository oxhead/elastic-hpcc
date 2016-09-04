import json

from elastic.hpcc import placement
from elastic.util import helper

locations = {
    "10.25.2.147": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_.*_people_.*._1_of_4",
    ],
    "10.25.2.148": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_.*_people_.*._2_of_4",
    ],
    "10.25.2.149": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_.*_people_.*._3_of_4",
    ],
    "10.25.2.151": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_.*_people_.*._4_of_4",
    ]
}

dp_file = "/home/chsu6/elastic-hpcc/mybenchmark/E14/placement/beta.json"
access_statistics = json.load(open(dp_file, 'r'))


helper.json_pretty_print(placement.PlacementTool.compute_node_statistics(access_statistics))
helper.json_pretty_print(placement.PlacementTool.compute_partition_statistics(access_statistics))
helper.json_pretty_print(placement.PlacementTool.convert_to_node_statistics(locations, access_statistics))