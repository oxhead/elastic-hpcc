for h in `cat ~/elastic-hpcc/.cluster_conf`
do
    (
    echo $h;
    ssh $h 'rm -rf /tmp/roxie; mkdir -p /tmp/roxie/bin; mkdir -p /tmp/roxie/libs'
    scp ~/hpcc_build/Debug/bin/roxie $h:/tmp/roxie/bin
    scp ~/hpcc_build/Debug/libs/libccd.so $h:/tmp/roxie/libs
    scp ~/hpcc_build/Debug/libs/libudplib.so $h:/tmp/roxie/libs
    ssh $h 'sudo cp /tmp/roxie/bin/roxie* /opt/HPCCSystems/bin'
    ssh $h 'sudo cp /tmp/roxie/libs/lib*.so /opt/HPCCSystems/lib'
    ) &
done

wait
