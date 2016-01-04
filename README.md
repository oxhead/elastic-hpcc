## [Project Description](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/)

## [Project Management](https://github.ncsu.edu/OSR/elastic-hpcc/wiki/Project-Management)


### Required Packages this project
* Linux
  - sshpass ( for ssh key deploymeny on VCL)

### How to setup a multi-node HPCC cluster
1. Create a list of hostnames
2. Make sure the current user has password-less ssh access to each node listed in above
```
vcl --hosts ~/list add_key
```
3. Install the HPCC software on each node (this step should also create a user, user)
```
hpcc --hosts ~/list install_package --package ~/build_osr/hpccsystems-platform-osr_6.0.0-alpha1Debugtrusty_amd64.deb
```
4. Make sure the hpcc user has password-less ssh aceess to the nodes too, e.g.
```
vcl --hosts ~/list deploy_key hpcc
```
5. Using Configuration Manager to create environment.xml
6. Deploy the generated configuration files to all the nodes, e.g.
```
hpcc --hosts ~/list deploy_config -c /etc/HPCCSystems/source/mycluster.xml
```
7. Clear the system setting, e.g.
```
hpcc --hosts ~/list clear_system
```
8. (Optional) Clear the system log, e.g.
```
hpcc --hosts ~/list clear_log
```
9. Start the HPCC service, e.g.
```
hpcc --hosts ~/list service --action start
```

