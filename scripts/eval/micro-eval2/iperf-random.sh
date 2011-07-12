#!/bin/bash

for i in `seq 1 100`
do

    eval `./select_two_random.py ~/hostnames-20.txt`
    ssh -i ~/key $HOST_FROM "iperf -c $HOST_TO" | tee -a auto-results/micro-iperf.txt

done