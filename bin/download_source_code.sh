#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# load the env variables
. $SCRIPT_DIR/env.sh

git submodule update --init --recursive
cd $REPO_DIR && git submodule update --init --recursive
