## [Project Description](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/)

  To prove distribution-aware dataplacement greatly improve system performance, e.g. throughput and latency.

## [Project Management](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/Project-Management)


### Required Packages for this project
* Git
  - sudo yum install git-all
* Linux
  - sudo apt-get install sshpass ( for ssh key deploymeny on VCL)
  - sudo apt-get build-dep python3-lxml ( for the lxml library in pip)
* Python
  - Python 3
  - Pip 3

    ```
    wget https://bootstrap.pypa.io/get-pip.py
    sudo python3 get-pip.py
    ```

  - Virtualenv

    ```
    sudo pip install virtualenv
    ```



### How to work with this tool set

1. cd to the directory with a clone of this repo
2. source init.sh



### How to setup a multi-node HPCC cluster

1. Create a list of hostnames, e.g. ~/list.public (a file with hostname per line)

2. Create an SSH key (skip if you don't need it)

3. Make sure the current user has password-less ssh access to each node listed in above

  ```
  vcl --hosts ~/list.public add_key
  ```

4. Create a list of private IP of servers (required on VCL)

  ```
  vcl --hosts ~/list.public ip > ~/list
  ```
5. Enable SSH login through the private ip

  ```
  vcl --hosts ~/list.public fix_ssh
  ```

6. Disable firewall

  ```
  vcl --hosts ~/list.public fix_firewall
  ```

7. Install and uninstall the HPCC software on each node (this step should also create a user, user)

  ```
  wget http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-6.0.0/bin/platform/hpccsystems-platform-community_6.0.0-beta1trusty_amd64.deb
  hpcc --hosts ~/list package install --deb ~/build_osr/hpccsystems-platform-osr_6.0.0-alpha1Debugtrusty_amd64.deb
  hpcc --hosts ~/list package uninstall
  ```
8. (optional) Deploy your own ssh key for the HPCC user, e.g.

  ```
  vcl --hosts ~/list deploy_key hpcc
  ```
9. Generate HPCC configurations 

   1. Using Configuration Manager to create environment.xml

      ```shell
      sudo /opt/HPCCSystems/sbin/configmgr
      # link to the browser, and create the configurations
      ```
   2. Using the commadline tool

      ```shell
      hpcc gen_config --output /tmp/mycluster.xml --overwrite --thor 5 --roxie 5 --channel_mode simple
      ```


1. Modify the configuration to avoid exception in some cases
  ```
  Add globalMemorySize="4096" to ThorCluster in the environment.xml
  ```

2. Deploy the generated configuration files to all the nodes, e.g.

  ```
  hpcc --hosts ~/list deploy_config -c /etc/HPCCSystems/source/mycluster.xml
  ```

3. (Optional) Clear the system setting, e.g.

  ```
  hpcc --hosts ~/list clear_system
  ```
4. (Optional) Clear the system log, e.g.

  ```
  hpcc --hosts ~/list clear_log
  ```
5. Start the HPCC service, e.g.

  ```
  hpcc --hosts ~/list service --action start
  ```



### How to run a Roxie query

1. Upload data to the Landing Zone, for example,

  ```
  hpcc ~/list upload_data --data ~/hpcc_dataset/OriginalPerson
  ```

2. Spray the data

   ```
   hpcc spray OriginalPerson tutorial:YN::OriginalPerson --dstcluster myroxie --recordsize 124
   hpcc spray 2of12.txt thor::word_list_csv --dstcluster mythor --format delimited --maxrecordsize 8192 --separator \ --terminator \n,\r\n --quote \'
   ```

3. Run a Roxie query without publishing it

  ```
  roxie run --ecl benchmark/OriginalPerson/count_person.ecl
  ```
4. Publish/unpublish a Roxie query

  ```
  roxie publish ValidateAnagrams --ecl benchmark/Anagram2/anagram2.ecl
  roxie unpublish ValidateAnagrams
  ```

5. Run a Roxie query

  ```
  roxie query ValidateAnagrams -i word teacher
  ```
6. Clean unused files (data partitions) on Roxie

  ```
  roxie clean_unused_files
  ```



### How to submit a job to Thor

1. Upload and spray the required input data as in running a Roxie query
2. Submit a Thor job




### How to run benchmark against the Roxie clsuter

1. Download the required dataset

  ```
  benchmark download_dataset
  ```
2. Execute the example scripts under **example/** to prepare data and Roxie queries

  ```

  ```
* bash example/run_anagram2.sh
* bash example/run_original_person.sh
* bash example/run_six_degree.sh
  ```

  ```

1. To run a stress test, do

  ```
  benchmark stress --times 20 --query validateanagrams --query searchlinks --query fetchpeoplebyzipservice --concurrency=2
  ```

2. To run a stress test in a distributed mode, do

  ```
  benchmark distributed_stress --query searchlinks --times 2 --concurrency 2
  ```



### How to run benchmark in the single node mode

1. Start the controller

  ```
  python script/start_controller.py conf/1driver.yaml conf/logging.yaml logs/
  ```

2. Start the driver

  ```
  python script/start_driver.py conf/1driver.yaml conf/logging.yaml logs/
  ```

3. Submit the workload

  ```
  benchmark submit conf/workload.yaml -o /tmp/test1
  ```




### How to run benchmark

1. Create a benchmark configuatino **conf/benchmark.yaml**
2. Deploy benchmark programs

  ```
  benchmark deploy
  ```

3. Install required packages

  ```
  benchmark --config conf/6driver.yaml install_package
  ```
4. Fix the permission on the controller node

   ```shell
   vcl --host [controller ip] fix_ssh
   vcl --host [controller ip] fix_sudotty
   vcl --host [controller ip] fix_firewall
   ```

5. Copy the benchmark configuarion

   ```shell
   benchmark --config conf/6driver.yaml deploy_config
   ```

6. Start all benchmark nodes (will copy the configuration to conf/benchmark.yaml)

   ```shell
   benchmark --config conf/6driver.yaml service start
   ```

7. Submit a workload (will return the workload id)

   ```shell
   benchmark --config conf/6driver.yaml submit conf/workload.yaml
   ```

8. Retrieve workload information

   ```shell
   benchmark workload [status|report|statistics] wid
   ```

### Handling node failures

1. Restart the node through VCL web portal

2. Set hostname (required?)

   ```shell
   for (( i=1;i<=10;i++ ))
   do
   	echo node$i
   	ssh node$i sudo hostname node$i
   done
   ```

3. Fix ssh

   ```shell
   vcl --hosts .cluster_conf fix_ssh; vcl --hosts .cluster_conf fix_firewall; vcl --hosts .cluster_conf fix_sudo; vcl --hosts .cluster_conf fix_sudotty
   ```
4. Fix HPCC configuration
  ```shell
  hpcc fix_coredump; hpcc increase_start_timeout
  ```

5. Mount NFS

   ```shell
   for h in `cat .cluster_conf`
   do
   	echo $h
   	ssh $h "sudo umount /dataset; sudo mkdir -p /dataset; sudo chown hpcc:hpcc /dataset; sudo mount 10.25.0.201:/VinceFreeh_HPCC/$h /dataset"
   done
   ```

### Prepare for Evaluation in Paper
1. Deploy configuration
   ```shell
   hpcc deploy_config -c template/elastic_1thor_8roxie_locality_nfs.xml
   ```
2. Reload topology cache
   ```shell
   hpcc --reload print_topology
   ```
3. 2. Clean system (if required)
   ```shell
   hpcc clear_system; hpcc clear_log
   ```
4. Start HPCC service
   ```shell
   hpcc service --action start
   ```
5. Generate required data (here 128 * 1GB partition)
   ```shell
   for (( i=1;i<=128;i++ )); do echo Generating partition $i; thor run --ecl benchmark/MyBenchmark_dynamic/GenData_dynamic.ecl --parameter id $i --wait_until_complete; done
   ```
6. Copy required data from Thor to Roxie (using NFS here)
   ```shell
   for h in `cat .cluster_conf`; do echo $h; ssh $h "sudo umount /dataset; sudo mkdir -p /dataset; sudo chown hpcc:hpcc /dataset; sudo mount 10.25.0.201:/VinceFreeh_HPCC/$h /dataset"; done
   ```
   ```shell
   for d in `cat .cluster_conf`; do sudo mkdir -p /dataset/$d/roxie/mybenchmark;  sudo chown -R systemd-bus-proxy:hpcc /dataset/$d; done
   ```
   ```shell
   for f in `ls /dataset/10.25.2.131/thor/mybenchmark`; do echo $f; for (( i=2;i<=9;i++ )); do echo copinyg $f to node$i; sudo cp /dataset/10.25.2.131/thor/mybenchmark/$f /dataset/`getent hosts node$i | awk '{ print $1 }'`/roxie/mybenchmark; done; done
   ```
7. Publish Roxie Query (no data will be copied to Roxie)
   ```shell
   for (( i=1;i<=128;i++ )); do roxie publish sequential_search_firstname_$i --ecl benchmark/MyBenchmark_dynamic/SequentialSearch_FirstName_$i.ecl; done
   ```
8. Generate required keys list for request workload
   ```shell
   python utils/generate_valid_keys.py benchmark/dataset/firstname_list_3068.txt
   ```
9. Deploy benchmark code
   ```shell
   benchmark --config conf/benchmark_template.yaml deploy
   ```
10. Install required packages for benchmark service
 ```shell
 benchmark --config conf/benchmark_template.yaml install_package
 ```
11. Run the evaluation program
   ```
   python mybenchmark/E24/run.py
   ```



### Note

* Increase the timeout for starting Roxie due to large number of published queries.  Changes from 120 to 500 seconds (need to address this for long waiting time).
* Disable core dump to prevent inaccessable to Roxie nodes.