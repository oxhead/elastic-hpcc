#!/bin/ash

VENV_DIR=.venv

action_prepare ()
{
    if [ ! -d "$VENV_DIR" ]; then
        virtualenv $VENV_DIR
        action_prepare
        bash --rcfile .venv/bin/activate
        pip install executor
        pip install click
        deactivate
    fi
}

action_init ()
{
    action_prepare
    bash --rcfile .venv/bin/activate
    pip install --editable sbin
}

### Main

action_init
