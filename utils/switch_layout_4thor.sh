#!/bin/bash

run_script='switch_layout_4thor.py'
for (( i=1;i<=10;i++ )); do scp utils/$run_script node$i:/tmp; done
for (( i=1;i<=10;i++ )); do ssh node$i "sudo python3 /tmp/$run_script"; done
