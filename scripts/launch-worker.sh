#!/bin/bash
BASE=$1
MASTER=$2

rm -rf /tmp/tmp*

mkdir -p $BASE/logs

ERROR_LOG=$BASE/logs/error.log
OUT_LOG=$BASE/logs/out.log

$BASE/scripts/run_worker.sh $MASTER 2>$ERROR_LOG 1>$OUT_LOG &
