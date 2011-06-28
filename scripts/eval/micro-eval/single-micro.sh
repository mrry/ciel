#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results

size=$1

function restart {

    scripts/sw-kill-cluster -f ~/hostnames-20.txt -i ~/key
    scripts/sw-run-command -f ~/hostnames-20.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*"
    sleep 3
    scripts/sw-launch-cluster -f ~/hostnames-20.txt -i ~/key $EXTRA_ARGS
    
    sleep 7
    
    curl http://localhost:8000/control/worker/ | grep netloc | wc -l
    
}

for i in `seq 1 10`
do

    exp_filename=auto-results/micro-single-yieldnolocal-$i.txt

    if [ ! -f $exp_filename ]
    then

	EXTRA_ARGS="--stopwatch"
	restart

	JOB_ID=`scripts/sw-submit src/package/test_scala_chainyield_randomsched.pack 1 5000 ChainYieldTest`
	echo $exp_filename
	until scripts/sw-wait $JOB_ID
	do
	    true
	done
	
	scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename

	curl $CIEL_MASTER/control/stopwatch/end_to_end > auto-results/`basename $exp_filename`-master-end_to_end.txt
	curl $CIEL_MASTER/control/stopwatch/master_task > auto-results/`basename $exp_filename`-master-master_task.txt

	for i in `cat ~/hostnames-20.txt`
	do

	    curl http://$i:8001/control/stopwatch/end_to_end > auto-results/`basename $exp_filename`-$i-end_to_end.txt
	    curl http://$i:8001/control/stopwatch/worker_task > auto-results/`basename $exp_filename`-$i-worker_task.txt

	done
	
    fi

done
