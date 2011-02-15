#!/bin/bash

HADOOP_HOME=/usr/lib/hadoop-0.20
HADOOP_CONF_DIR=/etc/hadoop-0.20/conf

wget http://apache.mirrors.timporter.net//mahout/0.3/mahout-0.3.tar.gz
wget http://mrry.s3.amazonaws.com/vecs100d_0
wget http://mrry.s3.amazonaws.com/clusters100d_0

tar xzvf mahout-0.3.tar.gz

for i in 20 40 60 80 100
do
    hadoop fs -mkdir /user/root/input-$i
    for j in `seq $i`
    do
        hadoop fs -copyFromLocal vecs100d_0 /user/root/input-$i/vecs-$j
    done
done

# Initial run to generate the seed.
mahout-0.3/bin/mahout kmeans --input /user/root/input-1 --k 100 --clusters /user/root/clusters-1 --output /user/root/output-1 --max 1 -r 1 -w

for i in 20 40 60 80 100
do 
    mahout-0.3/bin/mahout kmeans --input /user/root/input-$i --clusters /user/root/clusters-1 --output /user/root/output-$i --max 5 -r 1 -w
done
