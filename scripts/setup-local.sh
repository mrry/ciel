#!/bin/bash

# pre-accept Java license agreement :)
#echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

# install stuff
apt-get -qq -y update
apt-get -qq -y install python python-ply python-httplib2 python-simplejson
#apt-get -qq -y install python python-ply python-httplib2 python-simplejson python-cherrypy3 sun-java6-jre
wget http://mirrors.kernel.org/ubuntu/pool/main/p/python-support/python-support_1.0.3ubuntu1_all.deb
dpkg -i python-support_1.0.3ubuntu1_all.deb
wget http://mirrors.kernel.org/ubuntu/pool/universe/c/cherrypy3/python-cherrypy3_3.1.2-1_all.deb
dpkg -i python-cherrypy3_3.1.2-1_all.deb

