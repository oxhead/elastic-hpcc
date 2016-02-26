## [Project Description](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/)

## [Project Management](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/Project-Management)


### Required Packages this project
* Linux
  - sshpass ( for ssh key deploymeny on VCL)
* Python
  - Python 3
  - Pip 3
  - Virtualenv

### How to work with this tool set
1. cd to the directory with a clone of this repo
2. source init.sh

### How to setup a multi-node HPCC cluster
1. Create a list of hostnames, e.g. ~/list.public (a file with hostname per line)

1. Create an SSH key (skip if you don't need it)

1. Make sure the current user has password-less ssh access to each node listed in above

  ```
vcl --hosts ~/list add_key
  ```

1. Create a list of private IP of servers (required on VCL)

  ```
vcl --hosts ~/list.public ip > ~/list
  ```
1. Enable SSH login through the private ip

  ```
vcl --hosts ~/list.public fix_ssh
  ```
1. Install and uninstall the HPCC software on each node (this step should also create a user, user)

  ```
hpcc --hosts ~/list package install --deb ~/build_osr/hpccsystems-platform-osr_6.0.0-alpha1Debugtrusty_amd64.deb
hpcc --hosts ~/list package uninstall
  ```
1. (optional) Deploy your own ssh key for the HPCC user, e.g.

  ```
vcl --hosts ~/list deploy_key hpcc
  ```
1. Using Configuration Manager to create environment.xml

  ```
* sudo /opt/HPCCSystems/sbin/configmgr
* link to the browser, and create the configurations
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
hpcc spray OriginalPerson tutorial:YN::OriginalPerson --dstcluster myroxie --recordsize 124
  ```
1. Run the command (assume all the files at roxie under the current user on the Thor master)

  ```
hpcc roxie --ecl test.ecl
  ```

