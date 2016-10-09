## [Project Description](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/)

## [Project Management](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/Project-Management)


### Required Packages this project
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

1. Create an SSH key (skip if you don't need it)

1. Make sure the current user has password-less ssh access to each node listed in above

  ```
vcl --hosts ~/list.public add_key
  ```

1. Create a list of private IP of servers (required on VCL)

  ```
vcl --hosts ~/list.public ip > ~/list
  ```
1. Enable SSH login through the private ip

  ```
vcl --hosts ~/list.public fix_ssh
  ```

1. Disable firewall

  ```
vcl --hosts ~/list.public fix_firewall
  ```

1. Install and uninstall the HPCC software on each node (this step should also create a user, user)

  ```
wget http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-6.0.0/bin/platform/hpccsystems-platform-community_6.0.0-beta1trusty_amd64.deb
hpcc --hosts ~/list package install --deb ~/build_osr/hpccsystems-platform-osr_6.0.0-alpha1Debugtrusty_amd64.deb
hpcc --hosts ~/list package uninstall
  ```
1. (optional) Deploy your own ssh key for the HPCC user, e.g.

  ```
vcl --hosts ~/list deploy_key hpcc
  ```
1. Generate HPCC configurations 

  1. Using Configuration Manager to create environment.xml

    ```
* sudo /opt/HPCCSystems/sbin/configmgr
* link to the browser, and create the configurations
    ```

  1. Using the commadline tool

    ```
hpcc gen_config --output /tmp/mycluster.xml --overwrite --thor 5 --roxie 5 --channel_mode simple
    ```

1. Deploy the generated configuration files to all the nodes, e.g.

  ```
hpcc --hosts ~/list deploy_config -c /etc/HPCCSystems/source/mycluster.xml
  ```
1. (Optional) Clear the system setting, e.g.

  ```
hpcc --hosts ~/list clear_system
  ```
1. (Optional) Clear the system log, e.g.

  ```
hpcc --hosts ~/list clear_log
  ```
1. Start the HPCC service, e.g.

  ```
hpcc --hosts ~/list service --action start
  ```

### How to run a Roxie query
1. Upload data to the Landing Zone, for example,

  ```
hpcc ~/list upload_data --data ~/hpcc_dataset/OriginalPerson
  ```
1. Spray the data

  ```
* hpcc spray OriginalPerson tutorial:YN::OriginalPerson --dstcluster myroxie --recordsize 124
* hpcc spray 2of12.txt thor::word_list_csv --dstcluster mythor --format delimited --maxrecordsize 8192 --separator \\ --terminator \\n,\\r\\n --quote \'
  ```
1. Run a Roxie query without publishing it

  ```
roxie run --ecl benchmark/OriginalPerson/count_person.ecl
  ```
1. Publish/unpublish a Roxie query

  ```
* roxie publish ValidateAnagrams --ecl benchmark/Anagram2/anagram2.ecl
* roxie unpublish ValidateAnagrams
  ```

1. Run a Roxie query

  ```
roxie query ValidateAnagrams -q word teacher
  ```
1. Clean unused files (data partitions) on Roxie

  ```
roxie clean_unused_files
  ```

### How to submit a job to Thor
1. Upload and spray the required input data as in running a Roxie query
1. Submit a Thor job


### How to run benchmark against the Roxie clsuter
1. Download the required dataset

  ```
benchmark download_dataset
  ```
1. Execute the example scripts under **example/** to prepare data and Roxie queries

  ```
* bash example/run_anagram2.sh
* bash example/run_original_person.sh
* bash example/run_six_degree.sh
  ```

1. To run a stress test, do

  ```
benchmark stress --times 20 --query validateanagrams --query searchlinks --query fetchpeoplebyzipservice --concurrency=2
  ```

1. To run a stress test in a distributed mode, do

  ```
benchmark distributed_stress --query searchlinks --times 2 --concurrency 2
  ```

### How to run benchmark in the single node mode
1. Start the controller

  ```
python script/start_controller.py conf/1driver.yaml conf/logging.yaml logs/
  ```

1. Start the driver

  ```
python script/start_driver.py conf/1driver.yaml conf/logging.yaml logs/
  ```

1. Submit the workload

  ```
benchmark submit conf/workload.yaml -o /tmp/test1
  ```

### How to run benchmark
1. Create a benchmark configuatino **conf/benchmark.yaml**
1. Deploy benchmark programs

  ```
benchmark deploy
  ```

1. Install required packages

  ```
benchmark --config conf/6driver.yaml install_package
  ```

1. Copy the benchmark configuarion

  ```
benchmark --config conf/6driver.yaml deploy_config
  ```

1. Start all benchmark nodes (will copy the configuration to conf/benchmark.yaml)

  ```
benchmark --config conf/6driver.yaml service start
  ```

1. Submit a workload (will return the workload id)

  ```
benchmark --config conf/6driver.yaml submit conf/workload.yaml
  ```

1. Get workload info

  ```
benchmark workload [status|report|statistics] wid
  ```

# Run integrated benchmark suit
1. python mybenchmark/compare_distributions.py
