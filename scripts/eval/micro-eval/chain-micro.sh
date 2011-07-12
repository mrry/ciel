#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results

size=$1

for i in `seq 1 20`
do

    exp_filename=auto-results/micro-chainyield-$i.txt

    if [ ! -f $exp_filename ]
    then

	scripts/sw-kill-cluster -f ~/hostnames-20.txt -i ~/key
	scripts/sw-run-command -f ~/hostnames-20.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*"
	sleep 3
	scripts/sw-launch-cluster -f ~/hostnames-20.txt -i ~/key
		
	sleep 7

	curl http://localhost:8000/control/worker/ | grep netloc | wc -l

	JOB_ID=`scripts/sw-submit src/package/test_scala_chainyield.pack $i`
	echo $exp_filename
	until scripts/sw-wait $JOB_ID
	do
	    true
	done
	
	scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
	
    fi

    #sleep 30

    #scripts/sw-run-command -i ~/key -f ~/cluster.txt -c "rm -rf /mnt/store; mkdir -p /mnt/store/data"
    #sleep 5

done
