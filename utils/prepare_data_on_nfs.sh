#!/bin/bash

for (( i=17;i<=128;i++ )); do echo Generating partition $i; thor run --ecl benchmark/MyBenchmark_dynamic/GenData_dynamic.ecl --parameter id $i --wait_until_complete; done

for (( i=2;i<=128;i++ )); do roxie publish sequential_search_firstname_$i --ecl benchmark/MyBenchmark_dynamic/SequentialSearch_FirstName_$i.ecl; done


#for f in `ls /dataset/10.25.2.131/thor/mybenchmark`; do echo $f; for (( i=2;i<=9;i++ )); do echo copinyg $f to node$i; sudo cp /dataset/10.25.2.131/thor/mybenchmark/$f /dataset/`getent hosts node$i | awk '{ print $1 }'`/roxie/mybenchmark; done; done
