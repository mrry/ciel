# Copyright (c) 2010 Chris Smowton <chris.smowton@cl.cam.ac.uk>
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
from skywriting.runtime.object_cache import retrieve_object_for_ref
from shared.references import SWReferenceJSONEncoder,\
    json_decode_object_hook
import sys
import os
import simplejson
import httplib2
from urlparse import urljoin
from shared.references import SWURLReference

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-r", "--refs", action="store_true", dest="refs", help="Set this option to look up reference names in the master", default=False)
    parser.add_option("-j", "--json", action="store_true", dest="json", help="Set this option to use JSON pretty printing", default=False)
    (options, args) = parser.parse_args()
    
    if options.refs:
        ref_ids = args
        
        for ref_id in ref_ids:
            
            # Fetch information about the ref from the master.
            h = httplib2.Http()
            _, content = h.request(urljoin(options.master, '/refs/%s' % ref_id), 'GET')
            ref_info = simplejson.loads(content, object_hook=json_decode_object_hook)
            ref = ref_info['ref']
            
            if options.json:
                obj = retrieve_object_for_ref(ref, 'json', None)
                simplejson.dump(obj, sys.stdout, cls=SWReferenceJSONEncoder, indent=4)
                print
            else:
                fh = retrieve_object_for_ref(ref, 'handle', None)
                for line in fh:
                    sys.stdout.write(line)
                fh.close()
            
    else:
        urls = args    
        
        for url in urls:
            if options.json:
                obj = retrieve_object_for_ref(SWURLReference([url]), 'json', None)
                simplejson.dump(obj, sys.stdout, cls=SWReferenceJSONEncoder, indent=4)
                print
            else:
                fh = retrieve_object_for_ref(SWURLReference([url]), 'handle', None)
                print fh
                for line in fh:
                    sys.stdout.write(line)
                fh.close()
        
if __name__ == '__main__':
    main()
