#!/bin/bash


# Git related
sudo yum install git-all
sudo yum install git bash-completion

# HPCC build related
sudo yum -y groupinstall "Development Tools"

# Build sshpass
wget -O http://downloads.sourceforge.net/project/sshpass/sshpass/1.06/sshpass-1.06.tar.gz
tar xf sshpass-1.06.tar.gz
cd sshpass-1.06
./configure
make
sudo make install


# Build Python3
# http://ask.xmodulo.com/install-python3-centos.html
sudo yum install yum-utils
sudo yum-builddep python
curl -O https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz
tar xf Python-3.5.2.tgz
cd Python-3.5.0
./configure
make
sudo make install
