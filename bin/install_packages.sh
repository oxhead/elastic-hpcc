#!/bin/bash

# essential packages
sudo apt-get install cmake
sudo apt-get install bison
sudo apt-get install flex
sudo apt-get install binutils-dev
sudo apt-get install libiberty-dev
sudo apt-get install slapd
sudo apt-get install openldap-dev
sudo apt-get install libicu-dev
sudo apt-get install libxslt-dev
sudo apt-get install zlib1g-dev
sudo apt-get install libarchive-dev
sudo apt-get install libboost-all-dev
sudo apt-get install libssl-dev
sudo apt-get install libapr1-dev
sudo apt-get install libaprutil1-dev
sudo apt-get install clang

# fix TBB
sudo apt-get install libtbb-*

# install nodejs
sudo apt-get install curl
curl -sL https://deb.nodesource.com/setup | sudo bash -
sudo apt-get install -y nodejs
