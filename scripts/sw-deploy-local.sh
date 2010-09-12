#!/bin/bash

# shut up dpkg
export DEBIAN_FRONTEND="noninteractive"

# install stuff
# XXX this requires root privileges, which the $SWUSER might not have!
apt-get -qq -y update 1>&2 2>/dev/null
apt-get -qq -y install git-core curl 1>&2 2>/dev/null

# pre-accept Java license agreement :)
echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

# install more stuff
apt-get -qq -y install python python-ply python-httplib2 python-simplejson python-cherrypy3 python-pycurl 1>&2 2>/dev/null
apt-get -qq -y install openjdk-6-jre 1>&2 2>/dev/null

# git checkout
mkdir -p /opt
cd /opt
git clone -q http://github.com/mrry/skywriting.git
mkdir -p /opt/skywriting/logs
mkdir -p /opt/skywriting/journal
mkdir -p /opt/skywriting/store
