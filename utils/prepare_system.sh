#!/bin/bash


# Git related
sudo yum install git-all
sudo yum install git bash-completion

# HPCC build related
sudo yum -y groupinstall "Development Tools"
sudo yum -y install cmake
# Fedora
sudo yum install gcc-c++ gcc make fedora-packager cmake bison flex binutils-devel openldap-devel libicu-devel  xerces-c-devel xalan-c-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel
# CentOS 6.4
sudo yum install gcc-c++ gcc make bison flex binutils-devel openldap-devel libicu-devel libxslt-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel hiredis-devel numactl-devel libmysqlclient-dev libevent-devel tbb-devel

# Build sshpass
wget http://downloads.sourceforge.net/project/sshpass/sshpass/1.06/sshpass-1.06.tar.gz
tar xf sshpass-1.06.tar.gz
cd sshpass-1.06
./configure
make
sudo make install


# Build Python3
# http://ask.xmodulo.com/install-python3-centos.html
#sudo yum install yum-utils
#sudo yum-builddep python
#curl -O https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz
#tar xf Python-3.5.2.tgz
#cd Python-3.5.0
#./configure
#make
#sudo make install
sudo yum -y install epel-release
sudo yum -y install python34
sudo yum -y install python34-devel


# nodejs for packaging HPCC
curl --silent --location https://rpm.nodesource.com/setup | sudo bash -
sudo yum install -y gcc-c++ make && sudo yum install -y nodejs
