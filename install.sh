#!/bin/ash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH="${PYTHONPATH}:${DIR}"
VENV_DIR=${DIR}/.venv

action_prepare ()
{
    if [ ! -d "$VENV_DIR" ]; then
        virtualenv $VENV_DIR
    fi
    . ${VENV_DIR}/bin/activate
    pip install executor 2>&1 > /dev/null
    pip install click 2>&1 > /dev/null
    pip install pyzmq 2>&1 > /dev/null
    pip install gevent 2>&1 > /dev/null
    pip install requests 2>&1 > /dev/null
    pip install netifaces 2>&1 > /dev/null
    pip install pyyaml 2>&1 > /dev/null
    pip install numpy 2>&1 > /dev/null
    pip install scipy 2>&1 > /dev/null
    pip install --editable ${DIR}/sbin 2>&1 > /dev/null
    deactivate
}

action_init ()
{
    action_prepare
}

### Main

action_init
