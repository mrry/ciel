#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results


for i in 1600000 3200000 8000000 16000000 24000000 32000000
do

    for caching in true
    do

	exp_filename=auto-results/kmeans-naive-$i-$caching.txt

	if [ ! -f $exp_filename ]
	then

	    scripts/sw-kill-cluster -f ~/newhosts.txt -i ~/key
	    scripts/sw-run-command -f ~/newhosts.txt -i ~/key -c "rm -rf /mnt/store/data; mkdir -p /mnt/store/data; rm -rf /tmp/*"
	    sleep 3
	    scripts/sw-launch-cluster -f ~/newhosts.txt -i ~/key

	    sleep 7

	    curl http://localhost:8000/control/worker/ | grep netloc | wc -l

	    export NUM_VECTORS=$i
	    export CACHING=$caching

	    JOB_ID=`scripts/sw-submit kmeans.pack`
	    echo $exp_filename >> auto-results/filename-log.txt
	    echo $exp_filename
	    until scripts/sw-wait $JOB_ID
	    do
		true
	    done
	    
	    scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
	    



	fi

    done



done
