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
from skywriting.runtime.block_store import BlockStore, SWReferenceJSONEncoder
import sys
import os
import simplejson

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-j", "--json", action="store_true", dest="json", help="Set this option to use JSON pretty printing", default=False)
    (options, args) = parser.parse_args()
    
    urls = args    
    
    # Retrieves should work anyway; this lot is mainly needed to store stuff.
    bs = BlockStore("dummy_hostname", "0", None)
    
    for url in urls:
        if options.json:
            obj = bs.retrieve_object_by_url(url, 'json')
            simplejson.dump(obj, sys.stdout, cls=SWReferenceJSONEncoder, indent=4)
            print
        else:
            fh = bs.retrieve_object_by_url(url, 'handle')
            for line in fh:
                sys.stdout.write(line)
            fh.close()
        
if __name__ == '__main__':
    main()