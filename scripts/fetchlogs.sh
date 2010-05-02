#!/bin/bash
SWBASE="../*"
KEY=~/.amazon/fedorakeypair.pem
USER="root"

I=0
mkdir fetched_logs
while read line
do
    echo "Getting logs from instance $I"
    scp -q -i $KEY $USER@$line:logs/error.log fetched_logs/error_$I.log
    scp -q -i $KEY $USER@$line:logs/out.log fetched_logs/out_$I.log
    I=`expr $I + 1`
done

exit 0