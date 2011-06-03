#!/bin/bash


mkdir -p auto-results


for j in 1 2 3 4 5 6 7 8 9 10
do

    export J=j

    export clusterfile=cluster-1-$((j * 2)).txt
    
    echo $clusterfile
    
    
    



	(for k in 1
	do
	   
            export CIEL_MASTER=http://`head -n 1 $clusterfile`:8000/
 
	    exp_filename=auto-results/grep-$k-$J.txt

	    if [ ! -f $exp_filename ]
	    then
		
	    	JOB_ID=`scripts/sw-submit grep.pack`
	    	echo $exp_filename
	    	until scripts/sw-wait $JOB_ID
	    	do
	    	    true
	    	done
		
	    	scripts/sw-task-crawler http://$CIEL_MASTER:8000/control/job/$JOB_ID > $exp_filename
		
	    	#sleep 35
		
	    fi
	    
	done) &

    #sleep 30

    #scripts/sw-run-command -i ~/key -f ~/cluster.txt -c "rm -rf /mnt/store; mkdir -p /mnt/store/data"
    #sleep 5

done

wait