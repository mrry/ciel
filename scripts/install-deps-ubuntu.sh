#!/bin/bash

# list of packages to be installed (space-delimited)
PACKAGES="python python-ply python-httplib2 python-simplejson python-cherrypy3 python-pycurl curl lighttpd python-flup ant openjdk-6-jdk mono-devel mono-mcs m4 gawk"

# the JDK to install
JDK="openjdk-6-jre"

# shut up dpkg
export DEBIAN_FRONTEND="noninteractive"

# pre-accept Java license agreement :)
echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

apt-get -qq -y install $PACKAGES 1>&2 2>/dev/null
apt-get -qq -y install $JDK 1>&2 2>/dev/null

# Because the Debian defaults assume we actually want this thing as our default server...

update-rc.d lighttpd disable
service lighttpd stop
