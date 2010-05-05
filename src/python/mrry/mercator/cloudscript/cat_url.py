#!/usr/bin/python

import sys
from mrry.mercator.runtime.block_store import BlockStore

if len(sys.argv) < 3:
    print "Usage: cat_url.py url master_host:master_port"
    sys.exit(1)

bs = BlockStore("dummy_hostname", "0", None, None)
# Retrieves should work anyway; this lot is mainly needed to store stuff.

global_handle = bs.retrieve_object_by_url(sys.argv[1], "json")
worker_handle = bs.retrieve_object_by_url((global_handle[0]).urls[0], "json")

for future_ref in worker_handle:
    inner_url = bs.retrieve_object_by_url("http://%s/global_data/%d" % (sys.argv[2], (future_ref[0]).id), "json")
    fh = bs.retrieve_object_by_url(inner_url[0].urls[0], "handle")
    for line in fh:
        sys.stdout.write(line)
    fh.close()
    
