#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results

size=$1



for i in `seq 1 5`
    do

    exp_filename=auto-results/ft-master-iter-$i-nofail.txt

    if [ ! -f $exp_filename ]
    then


	export EXTRA_ARGS="-j /opt/skywriting/journal --task-log-root /tmp/master1/"

	rm -rf /opt/skywriting/journal/*
	
	scripts/sw-kill-cluster -f ~/hostnames-20.txt -i ~/key
	scripts/sw-run-command -f ~/hostnames-20.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*; rm /opt/skywriting/logs/*"
	sleep 3

	mkdir /tmp/master1

	touch /tmp/master1/ciel-task-log.txt

	scripts/sw-launch-cluster -f ~/hostnames-20.txt -i ~/key
	
	sleep 7
	
	curl http://localhost:8000/control/worker/ | grep netloc | wc -l

	JOB_ID=`scripts/sw-submit src/package/test_scala_iter.pack 100 20 3000`
	echo $exp_filename
	until scripts/sw-wait $JOB_ID
	do
	    true
	done
	
	cp /tmp/master1/ciel-task-log.txt auto-results/ft-master-iter-$i-nofail.txt

	#scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
	
    fi

done
