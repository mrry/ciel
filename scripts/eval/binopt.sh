#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results


for i in `seq 1 5`
do

    for t in 10 20 30 40 50 60 70 80 90 100 110 120 130 140 150 160 170 180 190 200
    do


	for n in 200000 400000 800000 1600000
	do


	    chunksize=`expr $n / $t`
	    echo $chunksize

	    exp_filename=auto-results/binopt-50-$n-$t-$i.txt

	    if [ ! -f $exp_filename ]
	    then
		
		JOB_ID=`scripts/sw-submit src/package/sw_binopt.pack $n $chunksize $i`
		echo $exp_filename
		until scripts/sw-wait $JOB_ID
		do
		    true
		done
		
		scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
		
		sleep 5

	    fi

	done

    done

    #sleep 30

    #scripts/sw-run-command -i ~/key -f ~/cluster.txt -c "rm -rf /mnt/store; mkdir -p /mnt/store/data"
    #sleep 5

done
