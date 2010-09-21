#!/bin/bash
N=100000
MASTER="http://bombjack-0.xeno.cl.cam.ac.uk:8000"
SWROOT="/local/scratch/ms705/skywriting/branch/skywriting"

for c in 6668; do
#for c in 10000 5000 2500 2000; do
    tasks=`expr $N / $c`
    echo "Running for chunk size $c ($tasks tasks): "
    export CHUNKSIZE=$c
    export TOTAL=$N
    export FOO=`date +%s`
    $SWROOT/scripts/sw-job -m $MASTER -e $SWROOT/src/sw/binomialoptions.sw 2>&1
done
