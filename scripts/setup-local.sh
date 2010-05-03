#!/bin/bash
ROLE=$1
BASE=$2

# pre-accept Java license agreement :)
#echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

# install stuff
apt-get -qq -y update
apt-get -qq -y install python python-ply python-httplib2 python-simplejson
#apt-get -qq -y install python python-ply python-httplib2 python-simplejson python-cherrypy3 sun-java6-jre

# install python packages that aren't in the cloudera AMIs
wget -q http://mirrors.kernel.org/ubuntu/pool/main/p/python-support/python-support_1.0.3ubuntu1_all.deb
dpkg -i python-support_1.0.3ubuntu1_all.deb 2>&1 1>/dev/null
wget -q http://mirrors.kernel.org/ubuntu/pool/universe/c/cherrypy3/python-cherrypy3_3.1.2-1_all.deb
dpkg -i python-cherrypy3_3.1.2-1_all.deb 2>&1 1>/dev/null

# unpack skywriting distribution
#mv sw-distrib.tar.gz ${BASE} 	# not sure if this is required
tar -xzf sw-distrib.tar.gz 2>&1 1>/dev/null

# spawn the appropriate kind of skywriting node
# N.B. $3 is the master hostname in the case of a worker
# and empty otherwise
${BASE}/launch-${ROLE}.sh ${BASE} $3
