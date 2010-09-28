#!/bin/bash
BASE=$1

mkdir -p $BASE/logs

ERROR_LOG=$BASE/logs/error.log
OUT_LOG=$BASE/logs/out.log

$BASE/scripts/run_master.sh 2>$ERROR_LOG 1>$OUT_LOG &
