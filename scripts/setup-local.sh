#!/bin/bash

# pre-accept Java license agreement :)
echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

# install stuff
apt-get -qq -y update
apt-get -qq -y install python python-ply python-httplib2 python-simplejson python-cherrypy3 sun-java6-jre
