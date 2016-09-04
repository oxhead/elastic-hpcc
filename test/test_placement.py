from elastic.hpcc import placement
from elastic.hpcc import roxie


partitions = [
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._4_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._1_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._2_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._3_of_4",
    "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._4_of_4",
]

access_statistics = {
    "node2": {
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._1_of_4": 5381,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._2_of_4": 267,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._3_of_4": 9,
    },
    "node3": {
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._1_of_4": 325,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._2_of_4": 12,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._1_of_4": 17340,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._2_of_4": 905,
    },
    "node4": {
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._3_of_4": 13,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._1_of_4": 10083,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._2_of_4": 537,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._3_of_4": 7,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._1_of_4": 976,
    },
    "node5": {
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._1_of_4": 2708,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._1_of_4": 73,
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._1_of_4": 6
    }
}

old_locations = {
    "node2": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._1_of_4",
    ],
    "node3": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._2_of_4",
    ],
    "node4": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_lastname._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_city._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_zip._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_firstname._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_lastname._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_city._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_unsorted_people_zip._3_of_4",
    ],
    "node5": [
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

old_nodes = ["node2", "node3", "node4", "node5"]
new_nodes = ["node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9"]


old_placement = placement.DataPlacement(old_nodes, partitions, old_locations)

dp = placement.CoarseGrainedDataPlacement.compute_optimal_placement(old_placement, new_nodes, access_statistics)
# dp = placement.FineGrainedDataPlacement.compute_optimal_placement(old_placement, new_nodes, access_statistics)


roxie.switch_data_placement(dp)

