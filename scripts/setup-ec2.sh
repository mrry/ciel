#!/bin/bash
SWBASE="../*"
if [[ $1 == "" ]]; then
    KEY=key.pem
else
    KEY=$1
fi
USER="root"

tar -cf sw-distrib.tar $SWBASE 2>/dev/null
gzip -q sw-distrib.tar 2>/dev/null

I=0
while read line
do
    echo -n "Setting up instance $I "
    scp -q -i $KEY setup-local.sh $USER@$line:
    scp -q -i $KEY launch-master.sh $USER@$line:
    scp -q -i $KEY launch-worker.sh $USER@$line:
    scp -q -i $KEY sw-distrib.tar.gz $USER@$line:
    if [ "$I" -eq "0" ]; then
	echo "... as a master."
	ssh -f -i $KEY $USER@$line "/root/setup-local.sh master /root"
	MASTER=$line
    else
	echo "... as a worker for $MASTER"
	ssh -f -i $KEY $USER@$line "/root/setup-local.sh worker /root $MASTER"
    fi
    I=`expr $I + 1`
done

exit 0