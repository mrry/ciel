#!/bin/bash
SWBASE="../*"
if [[ $1 == "" ]]; then
    KEY=key.pem
else
    KEY=$1
fi
USER="root"

I=0
while read line
do
    if [ "$I" -eq "0" ]; then
	echo "($I) Launching master."
	ssh -f -i $KEY $USER@$line "/root/launch-master.sh /root"
	MASTER=$line
    else
	echo "($I) Launching worker for $MASTER"
	ssh -f -i $KEY $USER@$line "/root/launch-worker.sh /root $MASTER"
    fi
    I=`expr $I + 1`
done

exit 0