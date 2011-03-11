#!/bin/bash
mkdir -p ext
cd ext
wget -N http://www.cl.cam.ac.uk/~dgm36/mahout/mahout-0.3.tar.bz2
if [ mahout-timestamp -ot mahout-0.3.tar.bz2 ]
then
    tar -x -j -v --file=mahout-0.3.tar.bz2 mahout-0.3/mahout-core-0.3.jar mahout-0.3/mahout-math-0.3.jar mahout-0.3/mahout-collections-0.3.jar mahout-0.3/lib/commons-logging-1.1.1.jar mahout-0.3/lib/slf4j-api-1.5.8.jar mahout-0.3/lib/slf4j-jcl-1.5.8.jar mahout-0.3/lib/uncommons-maths-1.2.jar mahout-0.3/lib/gson-1.3.jar mahout-0.3/lib/hadoop-core-0.20.2.jar
    touch -r mahout-0.3.tar.bz2 mahout-timestamp
fi

wget -N http://google-gson.googlecode.com/files/google-gson-1.6-release.zip
if [ gson-timestamp -ot google-gson-1.6-release.zip ]
then
    unzip google-gson-1.6-release.zip google-gson-1.6/gson-1.6.jar
    touch -r google-gson-1.6-release.zip gson-timestamp
fi