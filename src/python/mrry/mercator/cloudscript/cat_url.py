# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
    
