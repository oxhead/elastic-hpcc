#!/bin/bash

SCRIPT_DIR="$(dirname "$(dirname "$(readlink "$0")")")"
BUILD_DIR=`readlink -e $SCRIPT_DIR/build`
SRC_DIR=`readlink -e $SCRIPT_DIR/HPCC-Platform`

echo $BUILD_DIR
echo $SRC_DIR

mkdir -p $BUILD_DIR
cd $BUILD_DIR && cmake $SRC_DIR && cmake -DCMAKE_BUILD_TYPE=Debug $SRC_DIR && make && make package
