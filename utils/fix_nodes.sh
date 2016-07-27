#!/bin/bash

for (( i=1;i<=10;i++ )); do echo node$i; ssh node$i "sudo hostname node$i; sudo mount /dev/vdb /data"; done