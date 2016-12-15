import glob
import json

from elastic.hpcc import placement
from elastic.util import helper

DP_1R = {
    "10.25.2.131": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_1._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_2._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_3._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_4._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_5._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_6._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_7._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_8._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_9._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_10._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_11._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_12._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_13._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_14._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_15._1_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_16._1_of_4",
    ],
    "10.25.2.132": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_1._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_2._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_3._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_4._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_5._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_6._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_7._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_8._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_9._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_10._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_11._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_12._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_13._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_14._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_15._2_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_16._2_of_4",
    ],
    "10.25.2.133": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_1._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_2._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_3._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_4._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_5._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_6._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_7._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_8._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_9._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_10._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_11._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_12._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_13._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_14._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_15._3_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_16._3_of_4",
    ],
    "10.25.2.134": [
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_1._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_2._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_3._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_4._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_5._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_6._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_7._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_8._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_9._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_10._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_11._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_12._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_13._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_14._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_15._4_of_4",
        "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark/data_sorted_people_firstname_16._4_of_4",
    ]
}