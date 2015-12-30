#!/bin/bash


SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# load the env variables
. $SCRIPT_DIR/env.sh

mkdir -p $BUILD_DIR
cd $BUILD_DIR && cmake $REPO_DIR && cmake -DCMAKE_BUILD_TYPE=Debug $REPO_DIR && make && make package
