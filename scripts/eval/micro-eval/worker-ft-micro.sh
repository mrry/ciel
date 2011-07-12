#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results

size=$1

function restart {

    scripts/sw-kill-cluster -f ~/hostnames-20.txt -i ~/key
    scripts/sw-run-command -f ~/hostnames-20.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*; rm /opt/skywriting/logs/*"
    sleep 3
    scripts/sw-launch-cluster -f ~/hostnames-20.txt -i ~/key $EXTRA_ARGS
    
    sleep 7
    
    curl http://localhost:8000/control/worker/ | grep netloc | wc -l
    
}


for nkill in `seq 1 5`
do

    for i in `seq 1 5`
    do

    exp_filename=auto-results/ft-worker-kill-$nkill-iter-$i.txt

    if [ ! -f $exp_filename ]
    then

	restart

	rm /tmp/{launch,kill}-list-$i.txt
	echo `hostname -f` > /tmp/launch-list-$i.txt
	./select_n_random.py ~/hostnames-20.txt $nkill > /tmp/kill-list-$i.txt
	cat /tmp/kill-list-$i.txt >> /tmp/launch-list-$i.txt

	JOB_ID=`scripts/sw-submit src/package/test_scala_iter.pack 100 10 3000`
	echo $exp_filename
	sleep 40
	scripts/sw-kill-cluster -f /tmp/kill-list-$i.txt -i ~/key
	sleep 40
	scripts/sw-launch-cluster -f /tmp/launch-list-$i.txt -i ~/key --skip-master
	until scripts/sw-wait $JOB_ID
	do
	    true
	done
	
	scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
	
    fi

done

done