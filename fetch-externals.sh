#!/bin/bash
mkdir -p ext
cd ext
wget -N http://www.cl.cam.ac.uk/~dgm36/mahout/mahout-0.3.tar.bz2
if [ mahout-timestamp -ot mahout-0.3.tar.bz2 ]
then
    tar -x -j -v --file=mahout-0.3.tar.bz2 mahout-0.3/mahout-core-0.3.jar mahout-0.3/mahout-math-0.3.jar mahout-0.3/mahout-collections-0.3.jar mahout-0.3/lib/commons-logging-1.1.1.jar mahout-0.3/lib/slf4j-api-1.5.8.jar mahout-0.3/lib/slf4j-jcl-1.5.8.jar mahout-0.3/lib/uncommons-maths-1.2.jar mahout-0.3/lib/gson-1.3.jar mahout-0.3/lib/hadoop-core-0.20.2.jar
    touch -r mahout-0.3.tar.bz2 mahout-timestamp
fi

curl -v -z google-gson-1.7.1-release.zip http://google-gson.googlecode.com/files/google-gson-1.7.1-release.zip
if [ gson-timestamp -ot google-gson-1.7.1-release.zip ]
then
    unzip google-gson-1.7.1-release.zip google-gson-1.7.1/gson-1.7.1.jar
    touch -r google-gson-1.7.1-release.zip gson-timestamp
fi

wget -N http://www.digip.org/jansson/releases/jansson-2.0.1.tar.bz2
if [ jansson-timestamp -ot jansson-2.0.1.tar.bz2 ]
then
    echo "Rebuilding Jansson..."
    rm -r jansson-2.0.1
    rm -r jansson-install
    mkdir -p jansson-install
    tar xvjf jansson-2.0.1.tar.bz2
    cd jansson-2.0.1
    ./configure --prefix=`readlink -m ../jansson-install`
    make
    make install
    cd ..
    touch -r jansson-2.0.1.tar.bz2 jansson-timestamp
fi

wget -N http://pypi.python.org/packages/source/s/sendmsg/sendmsg-1.0.1.tar.gz
if [ sendmsg-timestamp -ot sendmsg-1.0.1.tar.gz ]
then
    echo "Installing sendmsg..."
    tar xvzf sendmsg-1.0.1.tar.gz
    cd sendmsg-1.0.1
    python setup.py build
    cd ..
    touch -r sendmsg-1.0.1.tar.gz sendmsg-timestamp
fi
    
