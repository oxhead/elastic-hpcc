import glob
import json

from elastic.hpcc import placement
from elastic.util import helper

DP_1R = {
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