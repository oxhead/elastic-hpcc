#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR=${SCRIPT_DIR}/../logs
mkdir -p ${LOG_DIR}
CONF_DIR=${SCRIPT_DIR}/../conf
BENCHMARK_CONF=${CONF_DIR}/benchmark.yaml

source ${SCRIPT_DIR}/../init.sh

python ${SCRIPT_DIR}/start_controller.py ${BENCHMARK_CONF} &> ${LOG_DIR}/controller.log &
