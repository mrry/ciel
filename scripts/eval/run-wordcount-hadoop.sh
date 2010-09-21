#!/bin/bash
HADOOP_HOME="/usr/lib/hadoop"
HADOOP_VER="0.20.2+320"
INPUT="swbs://domU-12-31-39-06-79-87.compute-1.internal:8001/wiki:index"

for i in 1 2 3 4 5; do
    echo "Running iteration $i: "
    time hadoop jar $HADOOP_HOME/hadoop-$HADOOP_VER-examples.jar wordcount $INPUT /user/root/wc-out/run$i "A27"
done
