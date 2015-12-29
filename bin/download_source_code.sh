#!/bin/bash

SRC_DIR=HPCC-Platform

git submodule update --init --recursive

cd $SRC_DIR; git submodule update --init --recursive
