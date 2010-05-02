#!/bin/bash
SWBASE="../run/mercator.hg/"
KEY="/home/ms705/.ssh/id_rsa"

I=0
while read -r line
do
    scp -i $KEY setupcache-local.sh $line:
    scp -i $KEY -r $SWBASE $line:
    ssh -i $KEY $line '/root/setup-local.sh'
    if [ $I -eq 0 ]; then
	ssh -i $KEY $line '/root/scripts/run_master.sh'
    else
	ssh -i $KEY $line '/root/scripts/run_worker.sh'
    fi
done

exit 0