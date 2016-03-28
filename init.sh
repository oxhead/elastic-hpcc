#!/bin/ash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH="${PYTHONPATH}:${DIR}"
VENV_DIR=.venv

action_prepare ()
{
    if [ ! -d "$VENV_DIR" ]; then
        virtualenv $VENV_DIR
    fi
    . .venv/bin/activate
    pip install executor 2>&1 > /dev/null
    pip install click 2>&1 > /dev/null
    pip install pyzmq 2>&1 > /dev/null
    pip install gevent 2>&1 > /dev/null
    pip install requests 2>&1 > /dev/null
    pip install --editable sbin 2>&1 > /dev/null
    pip install --editable bin 2>&1 > /dev/null
    pip install --editable benchmark 2>&1 > /dev/null
    deactivate
}

action_init ()
{
    action_prepare
    #bash --rcfile .venv/bin/activate
    . $VENV_DIR/bin/activate
}

### Main

action_init
