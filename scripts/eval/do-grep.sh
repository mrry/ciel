#!/bin/bash

export CIEL_MASTER=http://`hostname -f`:8000/

mkdir -p auto-results


for j in 1 2 3 4 5 6 7 8 9 10
do

	for k in 10
	do
	    
	    exp_filename=auto-results/grep-$k-$j.txt

	    if [ ! -f $exp_filename ]
	    then
		
	    	JOB_ID=`scripts/sw-submit grep.pack`
	    	echo $exp_filename
	    	until scripts/sw-wait $JOB_ID
	    	do
	    	    true
	    	done
		
	    	scripts/sw-task-crawler http://localhost:8000/control/job/$JOB_ID > $exp_filename
		
	    	#sleep 35
		
	    fi
	    
	done

    #sleep 30

    #scripts/sw-run-command -i ~/key -f ~/cluster.txt -c "rm -rf /mnt/store; mkdir -p /mnt/store/data"
    #sleep 5

done
