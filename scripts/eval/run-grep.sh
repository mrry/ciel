#!/bin/bash
MASTER="http://ec2-72-44-42-101.compute-1.amazonaws.com:8000"
SWROOT="/local/scratch/ms705/skywriting/branch/skywriting"
INPUT="swbs://domU-12-31-39-06-79-87.compute-1.internal:8001/wiki:index"

for i in 1 2 3 4 5; do
    echo "Running iteration $i: "
    export FOO=`date +%s`
    export DATA_REF=$INPUT
    time $SWROOT/scripts/sw-job -m $MASTER -e $SWROOT/src/sw/grep.sw 2>&1
    $SWROOT/scripts/sw-flush-workers -m $MASTER
done
