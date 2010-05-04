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
	MASTER=$line
    else
        echo -n "Killing instance $I "
	echo " (worker)."
	ssh -f -i $KEY $USER@$line "/root/pkill.sh python"
    fi
    I=`expr $I + 1`
done

# kill the master
echo "Killing the master."
ssh -f -i $KEY $USER@$MASTER "/root/pkill.sh python"

exit 0