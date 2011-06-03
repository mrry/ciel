#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results


for i in `seq 1 100`
do



    for k in 100 1
    do

	export K=$k

	exp_filename=auto-results/fixedsched-tworandom-sweet-cache-100-k-$k-$i.txt

	if [ ! -f $exp_filename ]
	then

	    JOB_ID=`scripts/sw-submit kmeans_fast_prepared.pack`
	    echo $exp_filename
	    until scripts/sw-wait $JOB_ID
	    do
		true
	    done
	    
	    scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename

	    sleep 35

	fi

    done

    #sleep 30

    #scripts/sw-run-command -i ~/key -f ~/cluster.txt -c "rm -rf /mnt/store; mkdir -p /mnt/store/data"
    #sleep 5

done
