#!/bin/bash

for (( i=1;i<=10;i++ )); do scp utils/switch_layout.py node$i:/tmp; done
for (( i=1;i<=10;i++ )); do ssh node$i "sudo python3 /tmp/switch_layout.py"; done
