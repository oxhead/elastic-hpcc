#!/bin/bash

sshd_config=/etc/ssh/sshd_config

sudo cp $sshd_config ${sshd_config}.bak
sudo sed -i "s/^AllowUsers.*/AllowUsers root `whoami` hpcc/" $sshd_config
