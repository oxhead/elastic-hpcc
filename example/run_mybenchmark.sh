#!/bin/bash

# 1) Upload data
#thor run --ecl benchmark/MyBenchmark/GenData.ecl --wait_until_complete

# 7) Publish the query
roxie publish bm_1_dataset_search --ecl benchmark/MyBenchmark/BM_1_Dataset_Search.ecl
roxie publish bm_2_dataset_join --ecl benchmark/MyBenchmark/BM_2_Dataset_Join.ecl
roxie publish bm_3_index_search --ecl benchmark/MyBenchmark/BM_3_Index_Search.ecl
roxie publish bm_4_index_search_simple --ecl benchmark/MyBenchmark/BM_4_Index_Search_Simple.ecl
roxie publish bm_5_index_join --ecl benchmark/MyBenchmark/BM_5_Index_Join.ecl
roxie publish bm_6_index_join --ecl benchmark/MyBenchmark/BM_6_Index_Join_Simple.ecl

