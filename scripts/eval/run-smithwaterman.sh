#!/bin/bash
NUM_ROWS=10
NUM_COLS=10
MASTER="http://bombjack-0.xeno.cl.cam.ac.uk:8000"
SWROOT="../.."

export INPUT1
export INPUT2
export NUM_ROWS
export NUM_COLS

for i in 1; do
#for c in 10000 5000 2500 2000; do
    echo "Running repetition $i: "
    export FOO=`date +%s`
    $SWROOT/scripts/sw-job -m $MASTER -e $SWROOT/src/sw/smithwaterman.sw 2>&1
done
