#!/bin/bash

cd /opt/skywriting

export PYTHONPATH=/opt/skywriting/src/python

for i in 20 40 60 80 100
do
    for j in `seq $i`
    do
        echo http://mrry.s3.amazonaws.com/vecs100d_0 >> urls-$i.txt
    done
done

echo http://mrry.s3.amazonaws.com/clusters100d_0 >> urls-clusters.txt

export CLUSTER_INPUT_URL=`scripts/sw-load -m http://localhost:8000/ -u urls-clusters.txt -r 5 | tee index-clusters.txt | head -n 1`

export VECS_20=`scripts/sw-load -m http://localhost:8000/ -u urls-20.txt -r 3 | tee index-20.txt | head -n 1`
export VECS_40=`scripts/sw-load -m http://localhost:8000/ -u urls-40.txt -r 3 | tee index-40.txt | head -n 1`
export VECS_60=`scripts/sw-load -m http://localhost:8000/ -u urls-60.txt -r 3 | tee index-60.txt | head -n 1`
export VECS_80=`scripts/sw-load -m http://localhost:8000/ -u urls-80.txt -r 3 | tee index-80.txt | head -n 1`
export VECS_100=`scripts/sw-load -m http://localhost:8000/ -u urls-100.txt -r 3 | tee index-100.txt | head -n 1`

export FOO=1

export VECTOR_INPUT_URL=$VECS_20
scripts/sw-job -e -m http://`hostname -f`:8000/ src/sw/skyhout-kmeans.sw

export VECTOR_INPUT_URL=$VECS_40
scripts/sw-job -e -m http://`hostname -f`:8000/ src/sw/skyhout-kmeans.sw

export VECTOR_INPUT_URL=$VECS_60
scripts/sw-job -e -m http://`hostname -f`:8000/ src/sw/skyhout-kmeans.sw

export VECTOR_INPUT_URL=$VECS_80
scripts/sw-job -e -m http://`hostname -f`:8000/ src/sw/skyhout-kmeans.sw

export VECTOR_INPUT_URL=$VECS_100
scripts/sw-job -e -m http://`hostname -f`:8000/ src/sw/skyhout-kmeans.sw
