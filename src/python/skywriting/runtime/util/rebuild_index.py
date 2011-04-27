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
from optparse import OptionParser
from skywriting.runtime.util.load import get_worker_netlocs,\
    upload_string_to_targets, select_targets
import httplib2
import os
import sys
import simplejson
from shared.references import SWReferenceJSONEncoder, json_decode_object_hook
import uuid

def main():
    
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-p", "--prefix", action="store", dest="prefix", help="Block name prefix", metavar="NAME", default=None)
    parser.add_option("-l", "--list", action="store_true", dest="list", help="Lists the pinned blocks in the cluster", default=False)
    parser.add_option("-r", "--replication", action="store", dest="replication", help="Copies of each block", type="int", metavar="N", default=1)
    (options, _) = parser.parse_args()

    if (options.prefix is None and not options.list) or options.master is None:
        parser.usage()
        sys.exit(-1)

    h = httplib2.Http()
    workers = get_worker_netlocs(options.master)

    

    if options.list:
        id_to_netloc = {}
        for netloc in workers:
            response, content = h.request('http://%s/control/admin/pin/' % netloc, 'GET')
            assert response.status == 200
            pin_set = simplejson.loads(content, object_hook=json_decode_object_hook)
            for ref in pin_set:
                try:
                    existing_set = id_to_netloc[ref.id]
                except KeyError:
                    existing_set = set()
                    id_to_netloc[ref.id] = existing_set
                existing_set.add(netloc)
            
        for id in sorted(id_to_netloc.keys()):
            print '%s\t%s' % (id, ", ".join(id_to_netloc[id]))

    else:
    
        id_to_ref = {}
        
        for netloc in workers:
            response, content = h.request('http://%s/control/admin/pin/' % netloc, 'GET')
            assert response.status == 200
            pin_set = simplejson.loads(content, object_hook=json_decode_object_hook)
            for ref in pin_set:
                if ref.id.startswith(options.prefix) and not ref.id.endswith('index'):
                    try:
                        existing_ref = id_to_ref[ref.id]
                        existing_ref.combine_with(ref)
                    except KeyError:
                        id_to_ref[ref.id] = ref
        
        sorted_ids = sorted(id_to_ref.keys())
        new_index = []
        for id in sorted_ids:
            new_index.append(id_to_ref[id])
    
        index_name = '%s:recovered_index' % str(uuid.uuid4())
        with open(index_name, 'w') as f:
            simplejson.dump(new_index, f, cls=SWReferenceJSONEncoder)
        print 'Wrote index to %s' % index_name
    
        targets = select_targets(workers, options.replication)
        upload_string_to_targets(simplejson.dumps(new_index, cls=SWReferenceJSONEncoder), index_name, targets)
        for target in targets:
            print 'swbs://%s/%s' % (target, index_name)
        
        
if __name__ == '__main__':
    main()
