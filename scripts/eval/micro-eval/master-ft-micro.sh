#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results

size=$1



for i in `seq 1 100`
    do

    exp_filename=auto-results/ft-master-iter-$i-post.txt

    if [ ! -f $exp_filename ]
    then


	export EXTRA_ARGS="-j /opt/skywriting/journal --task-log-root /tmp/master1/"

	rm -rf /opt/skywriting/journal/*
	
	scripts/sw-kill-cluster -f ~/hostnames-20.txt -i ~/key
	scripts/sw-run-command -f ~/hostnames-20.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*; rm /opt/skywriting/logs/*"
	sleep 3

	mkdir /tmp/master1
	mkdir /tmp/master2

	touch /tmp/master1/ciel-task-log.txt
	touch /tmp/master2/ciel-task-log.txt

	scripts/sw-launch-cluster -f ~/hostnames-20.txt -i ~/key
	
	sleep 7
	
	curl http://localhost:8000/control/worker/ | grep netloc | wc -l



	echo `hostname -f` > /tmp/master-list.txt

	rand --max 300 -s $i > auto-results/ft-master-iter-$i-sleep.txt

	JOB_ID=`scripts/sw-submit src/package/test_scala_iter.pack 100 20 3000`
	echo $exp_filename
	sleep `cat auto-results/ft-master-iter-$i-sleep.txt`
	scripts/sw-kill-cluster -f /tmp/master-list.txt -i ~/key
	sleep 10
	export EXTRA_ARGS="-j /opt/skywriting/journal --task-log-root /tmp/master2/"
	scripts/sw-launch-cluster -f /tmp/master-list.txt -i ~/key 
	sleep 5
	curl http://localhost:8000/control/job/$JOB_ID/resume/
	until scripts/sw-wait $JOB_ID
	do
	    true
	done
	
	cp /tmp/master1/ciel-task-log.txt auto-results/ft-master-iter-$i-pre.txt
	cp /tmp/master2/ciel-task-log.txt auto-results/ft-master-iter-$i-post.txt

	#scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
	
    fi

done
