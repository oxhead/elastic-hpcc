#!/bin/bash

PROJECT_DIR="$(dirname "$(dirname "$(readlink "$0")")")"
REPO_NAME=HPCC-Platform
SRC_DIR=`readlink -e $PROJECT_DIR/$REPO_NAME`
# The separate repo directory for development purpose. Assume in the same level with PROJECT_DIR
DEV_SRC_DIR=`readlink -e $PROJECT_DIR/../$REPO_NAME`
REPO_DIR=`if [ -d "$DEV_SRC_DIR" ]; then echo $DEV_SRC_DIR; else echo $SRC_DIR; fi`
BUILD_DIR=`readlink -e $PROJECT_DIR/build`
BIN_DIR=`readlink -e $PROJECT_DIR/bin`

echo "src: $SRC_DIR"
echo "bild: $BUILD_DIR"
echo "bin: $BIN_DIR"
echo "repo: $REPO_DIR"
