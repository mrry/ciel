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
from skywriting.runtime.block_store import sw_to_external_url,\
    get_netloc_for_sw_url, get_id_for_sw_url
import sys
import httplib2
import simplejson
from shared.references import SW2_ConcreteReference, json_decode_object_hook
from optparse import OptionParser

def main():
    
    parser = OptionParser()
    parser.add_option("-i", "--index", action="store", dest="index", help="Index SWBS URI", metavar="URI", default=None)
    parser.add_option("-b", "--block", action="store", dest="block", help="Block SWBS URI", metavar="URI", default=None)
    (options, _) = parser.parse_args()

    h = httplib2.Http()
    
    if options.block is not None:
        
        netloc = get_netloc_for_sw_url(options.block)
        id = get_id_for_sw_url(options.block)
        
        response, _ = h.request('http://%s/admin/pin/%s' % (netloc, id), 'POST', 'pin')
        assert response.status == 200
        print >>sys.stderr, 'Pinned block %s to %s' % (id, netloc)
        
    if options.index is not None:
    
        index_url = sw_to_external_url(options.index)
        
        _, content = h.request(index_url, 'GET')
        index = simplejson.loads(content, object_hook=json_decode_object_hook)
        
        for chunk in index:
            assert isinstance(chunk, SW2_ConcreteReference)
            for netloc in chunk.location_hints:
                response, _ = h.request('http://%s/admin/pin/%s' % (netloc, chunk.id), 'POST', 'pin')
                assert response.status == 200
                print >>sys.stderr, 'Pinned block %s to %s' % (chunk.id, netloc)
                    
if __name__ == '__main__':
    main()
