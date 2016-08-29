import glob
import json

from elastic.hpcc import placement
from elastic.util import helper


def main():
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
    old_locations = {
        "10.25.2.147": [
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._1_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._1_of_4",
        ],
        "10.25.2.148": [
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._2_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._2_of_4",
        ],
        "10.25.2.149": [
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._3_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._3_of_4",
        ],
        "10.25.2.151": [
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._4_of_4",
            "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._4_of_4",
        ]
    }




    new_nodes = ['10.25.2.147', '10.25.2.148', '10.25.2.149', '10.25.2.151', '10.25.2.152', '10.25.2.153', '10.25.2.157', '10.25.2.131']
    for dp_path in glob.glob("/home/chsu6/elastic-hpcc/mybenchmark/E14/placement/*.json"):
        print(dp_path)
        access_statistics = json.load(open(dp_path, 'r'))


        old_nodes = list(old_locations.keys())
        old_partitions = set(placement.PlacementTool.compute_partition_statistics(access_statistics).keys())
        old_dp = placement.DataPlacement(old_nodes, old_partitions, old_locations)

        new_dp = placement.CoarseGrainedDataPlacement.compute_optimal_placement(old_dp, new_nodes, access_statistics)
        #new_dp = placement.FineGrainedDataPlacement.compute_optimal_placement(old_dp, new_nodes, access_statistics)

        helper.json_pretty_print(new_dp.locations)


if __name__ == "__main__":
    main()