#!/usr/bin/python

import subprocess
import sys

data_uris = []
sample_uris = []

mode = 0

for filename in sys.argv[1:]:

    with open(filename, "r") as records:
        
        for record in records:

            if record[:4] == "Data":
                mode = 1
                continue
            elif record[:4] == "Samp":
                mode = 2
                continue
            else:
                if mode == 1:
                    data_uris.append(record)
                elif mode == 2:
                    sample_uris.append(record)
    mode = 0

data_file = open("/tmp/data_json", "w")
sample_file = open("/tmp/sample_json", "w")

def write_ref(f, u, size):
    f.write(" { \"__ref__\": [ \"urls\", [\"%s\"], %d ] } " % (u[:-1], size))

data_file.write("[")
write_ref(data_file, data_uris[0], 25000000)
for duri in data_uris[1:]:
    data_file.write(", ")
    write_ref(data_file, duri, 25000000)
data_file.write("]")

sample_file.write("[")
write_ref(sample_file, sample_uris[0], 200)
for suri in sample_uris[1:]:
    sample_file.write(", ")
    write_ref(sample_file, suri, 200)
sample_file.write("]")

#print "sample_input_refs = [", ("ref(\"%s\")" % sample_uris[0][:-1])
#for sample_uri in sample_uris[1:]:
#    print (", ref(\"%s\")" % sample_uri[:-1])
#print "];"

#print "input_refs = [", ("ref(\"%s\")" % data_uris[0][:-1])
#for data_uri in data_uris[1:]:
#    print (", ref(\"%s\")" % data_uri[:-1])
#print "]"
