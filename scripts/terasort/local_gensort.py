#!/usr/bin/python

from __future__ import with_statement

import sys
import subprocess
import simplejson
import httplib2
import tempfile
import urlparse
import os
import socket

start_record = int(sys.argv[1])
records = int(sys.argv[2])

http = httplib2.Http()
tempdir = tempfile.mkdtemp()

devnull = open("/dev/null", "w")

wget_proc = subprocess.Popen(["wget", "http://www.ordinal.com/try.cgi/gensort-1.2.tar.gz", "-O", tempdir + "/gensort.tar.gz"])
wget_proc.wait()

tar_proc = subprocess.Popen(["tar", "xvzf", "gensort.tar.gz"], cwd=tempdir, stdout=devnull)
tar_proc.wait()

apt_proc = subprocess.Popen(["apt-get", "-qq", "-y", "install", "zlib1g-dev"], stdout=devnull)
apt_proc.wait()

make_proc = subprocess.Popen(["make"], stdout=devnull, cwd=(tempdir + "/gensort"))
make_proc.wait()

chunks = records / 2500000
chunk_size = 2500000
sys.stderr.write("Creating %d chunks\n" % chunk_size)

data_uris = []
sample_uris = []

for i in range(chunks):

    tf_name = tempfile.mktemp(dir=tempdir)
    gensort_args = [tempdir + "/gensort/gensort", "-a", "-b%d" % (start_record + (i * chunk_size)), str(chunk_size), tf_name]
    sys.stderr.write("Running %s\n" % gensort_args)
    gensort_proc = subprocess.Popen(gensort_args)
    gensort_proc.wait()
    worker_data_uri = urlparse.urljoin("http://" + socket.gethostname() + ":9001", "/data/")
    with open(tf_name, "r") as gensort_out:
        data = gensort_out.read()
        #print "Writing", data
        sys.stderr.write("Uploading %d bytes for chunk %d (%s)\n" % (len(data), i, worker_data_uri))
        (response, content) = http.request(worker_data_uri, "POST", data) #, headers={'Content-Length':str(len(data))})
        stored_input_uri, size_hint = simplejson.loads(content)
        data_uris.append(stored_input_uri)
        sys.stderr.write("Stored as %s\n" % stored_input_uri)
        #print "Head temporary file", tf2.file.name
        head_proc = subprocess.Popen(["head", "-n", str(1000 / chunks), tf_name], stdout=subprocess.PIPE)
        #print "Writing sample", tf2.read()
        head_data = head_proc.stdout.read()
        sys.stderr.write("Uploading %d bytes for chunk %d's sample\n" % (len(head_data), i))
        (sampleresponse, samplecontent) = http.request(worker_data_uri, "POST", head_data)
        stored_sample_uri, sample_size_hint = simplejson.loads(samplecontent)
        sys.stderr.write("Stored as %s\n" % stored_sample_uri)
        sample_uris.append(stored_sample_uri)
    os.remove(tf_name)

print "Data---"

for data_uri in data_uris:
    print data_uri

print "Samples---"

for sample_uri in sample_uris:
    print sample_uri
    
