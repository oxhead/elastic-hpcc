#!/bin/bash

for h in `cat ~/node.list.public`; do echo $h; ssh -tt $h "dstat --version"; done
