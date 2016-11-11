#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 1) Upload data
#thor run --ecl ${DIR}/GenData_16.ecl --wait_until_complete

# 2) Publish the query
for (( i=1;i<=16;i++ ));
do
    roxie publish sequential_search_firstname_$i --ecl ${DIR}/SequentialSearch_FirstName_$i.ecl
done
