#!/bin/ash

VENV_DIR=.venv

action_prepare ()
{
    if [ ! -d "$VENV_DIR" ]; then
        virtualenv $VENV_DIR
    fi
    . .venv/bin/activate
    pip install executor 2>&1 > /dev/null
    pip install click 2>&1 > /dev/null
    pip install --editable sbin 2>&1 > /dev/null
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
