#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 1) Upload data
#thor run --ecl ${DIR}/MyBenchmark/GenData.ecl --wait_until_complete
#thor run --ecl ${DIR}/MyBenchmark/GenData_firstname.ecl --wait_until_complete
#thor run --ecl ${DIR}/MyBenchmark/GenData_random.ecl --wait_until_complete

# 2) Publish the query
roxie publish sequential_search_firstname --ecl ${DIR}/MyBenchmark/SequentialSearch_FirstName.ecl
roxie publish sequential_search_lastname --ecl ${DIR}/MyBenchmark/SequentialSearch_LastName.ecl
#roxie publish sequential_search_state --ecl ${DIR}/MyBenchmark/SequentialSearch_State.ecl
roxie publish sequential_search_city --ecl ${DIR}/MyBenchmark/SequentialSearch_City.ecl
roxie publish sequential_search_zip --ecl ${DIR}/MyBenchmark/SequentialSearch_Zip.ecl

roxie publish random_search_firstname --ecl ${DIR}/MyBenchmark/RandomSearch_FirstName.ecl
roxie publish random_search_lastname --ecl ${DIR}/MyBenchmark/RandomSearch_LastName.ecl
#roxie publish random_search_state --ecl ${DIR}/MyBenchmark/RandomSearch_State.ecl
roxie publish random_search_city --ecl ${DIR}/MyBenchmark/RandomSearch_City.ecl
roxie publish random_search_zip --ecl ${DIR}/MyBenchmark/RandomSearch_Zip.ecl

