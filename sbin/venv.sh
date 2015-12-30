#!/bin/bash

action_prepare ()
{
    echo 'prepare'
    virtualenv .venv
    . .venv/bin/activate
    pip install executor
    deactivate
}

### Main

action=$1

case "$action" in
    prepare)
        action_prepare
        ;;
    *)
        echo $"Usage: $0 {prepare}"
        exit 1
esac
