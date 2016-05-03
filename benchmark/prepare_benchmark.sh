#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 1) Upload data
thor run --ecl ${DIR}/MyBenchmark/GenData.ecl --wait_until_complete

# 7) Publish the query
roxie publish bm_1 --ecl ${DIR}/MyBenchmark/BM_1_Dataset_Search.ecl
roxie publish bm_2 --ecl ${DIR}/MyBenchmark/BM_2_Dataset_Join.ecl
roxie publish bm_3 --ecl ${DIR}/MyBenchmark/BM_3_Index_Search.ecl
roxie publish bm_4 --ecl ${DIR}/MyBenchmark/BM_4_Index_Search_Simple.ecl
roxie publish bm_5 --ecl ${DIR}/MyBenchmark/BM_5_Index_Join.ecl
roxie publish bm_6 --ecl ${DIR}/MyBenchmark/BM_6_Index_Join_Simple.ecl

