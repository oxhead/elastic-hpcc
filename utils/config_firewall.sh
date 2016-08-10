#!/bin/bash

firewall-cmd --zone=internal --add-port=9996/tcp --permanent
firewall-cmd --zone=internal --add-port=9997/tcp --permanent
firewall-cmd --zone=internal --add-port=9998/tcp --permanent
firewall-cmd --zone=internal --add-port=9999/tcp --permanent
firewall-cmd --zone=internal --change-interface=enp5s1f0
