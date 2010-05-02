#!/bin/bash
SWBASE="../*"
KEY=~/.ec2/malteskey.pem
USER="root"

I=0
while read line
do
    echo "Getting logs from instance $I"
    scp -q -i $KEY $USER@$line:logs/error.log logs/error_$I.log
    scp -q -i $KEY $USER@$line:logs/out.log logs/out_$I.log
    I=`expr $I + 1`
done

exit 0