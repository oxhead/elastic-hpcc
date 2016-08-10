for h in `cat ~/elastic-hpcc/.cluster_conf`
do
    (
    echo $h;
    ssh $h "rm -rf /tmp/roxie; mkdir -p /tmp/roxie/bin; mkdir -p /tmp/roxie/libs"
    echo ssh $h "mkdir -p /tmp/roxie/bin; mkdir -p /tmp/roxie/libs"
    scp ~/hpcc_build/Debug/bin/* $h:/tmp/roxie/bin
    scp ~/hpcc_build/Debug/libs/* $h:/tmp/roxie/libs
    #scp hpcc_build/Debug/libs/libccd.so $h:/tmp/libs
    #scp hpcc_build/Debug/libs/libudplib.so $h:/tmp/libs
    ssh $h 'sudo cp /tmp/roxie/bin/* /opt/HPCCSystems/bin'
    ssh $h 'sudo cp /tmp/roxie/libs/* /opt/HPCCSystems/lib'
    ) &
done

wait
