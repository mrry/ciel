# Copyright (c) 2010 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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

import httplib2
import sys
from Queue import Queue, Empty
from skywriting.runtime.block_store import json_decode_object_hook
import simplejson
from urlparse import urljoin

def main():
    
    root_url = sys.argv[1]
    
    h = httplib2.Http()
    
    q = Queue()
    q.put(root_url)
    
    print 'task_id type parent created_at assigned_at committed_at duration num_children num_dependencies num_outputs final_state'
    
    while True:
        try:
            url = q.get(block=False)
        except Empty:
            break
        _, content = h.request(url)
        
        descriptor = simplejson.loads(content, object_hook=json_decode_object_hook)

        task_id = descriptor["task_id"]
        parent = descriptor["parent"]

        created_at = None
        assigned_at = None
        committed_at = None

        for (time, state) in descriptor["history"]:
            if state == 'CREATED':
                created_at = time
            elif assigned_at is None and (state == 'ASSIGNED' or state == 'ASSIGNED_STREAMING'):
                assigned_at = time
            elif state == 'COMMITTED':
                committed_at = time

        duration = committed_at - assigned_at if (committed_at is not None and assigned_at is not None) else None

        num_children = len(descriptor["children"])

        num_dependencies = len(descriptor["dependencies"])

        num_outputs = len(descriptor["expected_outputs"])

        type = descriptor["handler"]

        final_state = descriptor["state"]

        print task_id, type, parent, created_at, assigned_at, committed_at, duration, num_children, num_dependencies, num_outputs, final_state

        for child in descriptor["children"]:
            q.put(urljoin(url, child))
            
if __name__ == '__main__':
    main()