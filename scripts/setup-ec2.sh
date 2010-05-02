#!/bin/bash
SWBASE="../*"
KEY=~/.amazon/fedorakeypair.pem
USER="root"

I=0
while read line
do
    echo -n "Setting up instance $I "
#    scp -q -i $KEY setup-local.sh $USER@$line:
#    scp -q -i $KEY launch-master.sh $USER@$line:
#    scp -q -i $KEY launch-worker.sh $USER@$line:
    scp -q -i $KEY -r $SWBASE $USER@$line:
#    ssh -f -i $KEY $USER@$line '/root/setup-local.sh'
    if [ "$I" -eq "0" ]; then
	echo "... as a master."
	ssh -f -i $KEY $USER@$line "/root/launch-master.sh /root"
	MASTER=$line
    else
	echo "... as a worker for $line"
	ssh -f -i $KEY $USER@$line "/root/launch-worker.sh /root $MASTER"
    fi
    I=`expr $I + 1`
done

exit 0