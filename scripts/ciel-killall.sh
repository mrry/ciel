#!/bin/bash

PROCS=`pgrep -f ciel-process-aaca0f5eb4d2d98a6ce6dffa99f8254b -d ,`

if [[ $PROCS == "" ]]; then
    echo "No tagged Ciel processes running"
    exit 1
else
    PGRPS=`ps --pid $PROCS --no-headers -o pgrp | awk --assign ORS=, "/\w+/ {gsub(/[[:space:]]*/,\"\",\\$0); print \\$0}"`
    pkill -9 -g $PGRPS
fi