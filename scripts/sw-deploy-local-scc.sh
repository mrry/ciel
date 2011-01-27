#!/bin/sh
# Copyright (c) 2010 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# ----
#
# Skywriting/Ciel local deployment helper script - not to be invoked manually. This
# is run by sw-deploy.sh after being copied to cluster machines. 
# Add any package installations or initial (one-off) setup tasks to this
# script.
# N.B.: This script assumes root privileges are given. Need to make sure that 
# the user indeed has them. Later, we probably want to add a check to ensure 
# this and otherwise throw an error.
#

# list of packages to be installed (space-delimited)
#PACKAGES="python python-ply python-httplib2 python-simplejson python-cherrypy3 python-pycurl"

# the JDK to install
#JDK="openjdk-6-jre"

if [[ $1 == '' ]]; then
    SWROOT='/opt/skywriting'
else
    SWROOT=$1
fi

if [[ $2 == '' ]]; then
    GITUSER='mrry'
else
    GITUSER=$2
fi

# shut up dpkg
export DEBIAN_FRONTEND="noninteractive"

# install stuff
# XXX this requires root privileges, which the $SWUSER might not have!
#apt-get -qq -y update 1>&2 2>/dev/null
#apt-get -qq -y install git-core curl 1>&2 2>/dev/null

# pre-accept Java license agreement :)
#echo sun-java6-jre shared/accepted-sun-dlj-v1-1 boolean true | debconf-set-selections

# install more stuff
#apt-get -qq -y install $PACKAGES 1>&2 2>/dev/null
#apt-get -qq -y install $JDK 1>&2 2>/dev/null

# git checkout
#git clone -q http://github.com/$2/skywriting.git $1

# for the SCC, copy the python installation over from NFS
SNFSROOT='/shared/ms705'
mkdir -p /opt/python/
cp $SNFSROOT/python.tar.gz /opt/python/
cd /opt/python
tar -xzf python.tar.gz
rm python.tar.gz

# for the SCC, copy some shared libraries from NFS
cp -r $SNFSROOT/python/usr/local/lib/libcurl.* /lib/
cp -r $SNFSROOT/python/usr/local/lib/libz.* /lib/

# for the SCC, copy the checkout from the NFS share
mkdir -p $1
cp /shared/ms705/ciel.tar.gz $1
cd $1
tar -xzf ciel.tar.gz
mkdir -p $1/logs
mkdir -p $1/journal
mkdir -p $1/store
#mkdir -p /mnt/store
#ln -s /mnt/store /opt/skywriting/store
