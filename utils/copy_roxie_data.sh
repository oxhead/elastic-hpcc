#!/bin/bash

for (( i=1;i<=1024;i++ ))
do
    echo app$i
    while [ ! -f /dataset/node2/thor/mybenchmark/data_sorted_people_firstname_$i._1_of_1 ] || [ ! -f /dataset/node2/thor/mybenchmark/idx_sorted_people_firstname_$i._1_of_2 ] || [ ! -f /dataset/node2/thor/mybenchmark/idx_sorted_people_firstname_$i._2_of_2 ]
    do
        echo "waiting for app$i to complete..."
        sleep 10
    done
    echo 'data files are generated'
    cd /dataset/node2/thor/mybenchmark/
    for f in `ls *_sorted_people_firstname_$i.*`
    do
        echo "processing $f"
        for (( n=2;n<=9;n++ ))
        do
            if [ ! -f /dataset/node$n/roxie/mybenchmark/$f ]
            then
                echo "copying $f to /dataset/node$n/roxie/mybenchmark"
                sudo scp /dataset/node2/thor/mybenchmark/$f /dataset/node$n/roxie/mybenchmark/
            fi
        done
    done
done
