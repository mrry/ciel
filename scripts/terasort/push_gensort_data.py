#!/usr/bin/python

from __future__ import with_statement
import urlparse
import httplib2
import sys
import tempfile
import subprocess
import simplejson
import os


if __name__ == '__main__':
    
    #print sys.argv
    
    machines_filename = sys.argv[1]
    n_mappers = int(sys.argv[2])
    n_reducers = int(sys.argv[3])
    records_per_mapper = int(sys.argv[4])

    total_records = records_per_mapper * n_mappers
    sample_records = 100000
    sample_records_per_mapper = sample_records / n_mappers

    last_record = 0
    machines_file = open(machines_filename, "r")

    print ("num_bucketers = %d;" % n_mappers)
    print ("num_mergers = %d;" % n_reducers)
    print "sample_records = %d;" % sample_records

    data_uris = []
    sample_uris = []

    machine_index = 0
    machines = []
    for machine in machines_file:
        sys.stderr.write("Using machine %s" % machine)
        machines.append(machine)

    for i in range(n_mappers):
        
        tf_name = tempfile.mktemp()
        #print "Temporary file", tf_name
        http = httplib2.Http()
        gensort_args = ["gensort", "-a", "-b%d" % last_record, str(records_per_mapper), tf_name]
        sys.stderr.write("Running %s\n" % gensort_args)
        gensort_proc = subprocess.Popen(gensort_args)
        gensort_proc.wait()
        last_record += records_per_mapper
        worker_data_uri = urlparse.urljoin("http://" + (machines[machine_index])[:-1], "/data/")
        with open(tf_name, "r") as gensort_out:
            data = gensort_out.read()
            #print "Writing", data
            sys.stderr.write("Uploading %d bytes for mapper %d (%s)\n" % (len(data), i, worker_data_uri))
            (response, content) = http.request(worker_data_uri, "POST", data) #, headers={'Content-Length':str(len(data))})
            stored_input_uri, size_hint = simplejson.loads(content)
            data_uris.append(stored_input_uri)
            sys.stderr.write("Stored as %s\n" % stored_input_uri)
            #print "Head temporary file", tf2.file.name
            head_proc = subprocess.Popen(["head", "-n", str(sample_records_per_mapper), tf_name], stdout=subprocess.PIPE)
            #print "Writing sample", tf2.read()
            head_data = head_proc.stdout.read()
            sys.stderr.write("Uploading %d bytes for mapper %d's sample\n" % (len(head_data), i))
            (sampleresponse, samplecontent) = http.request(worker_data_uri, "POST", head_data)
            stored_sample_uri, sample_size_hint = simplejson.loads(samplecontent)
            sys.stderr.write("Stored as %s\n" % stored_sample_uri)
            sample_uris.append(stored_sample_uri)
            machine_index = machine_index + 1
            if machine_index >= len(machines):
                machine_index = 0
        os.remove(tf_name)


    print "sample_input_refs = [", ("ref(\"%s\")" % sample_uris[0])
    for sample_uri in sample_uris[1:]:
        print (", ref(\"%s\")" % sample_uri)
    print "];"

    print "input_refs = [", ("ref(\"%s\")" % data_uris[0])
    for data_uri in data_uris[1:]:
        print (", ref(\"%s\")" % data_uri)
    print "];"
