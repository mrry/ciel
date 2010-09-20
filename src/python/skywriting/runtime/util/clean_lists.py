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
import simplejson
import sys
from skywriting.runtime.block_store import json_decode_object_hook
from skywriting.runtime.references import SW2_ConcreteReference
import os

def main():
    
    # Read in the JSON on standard in.
    index = simplejson.load(sys.stdin, object_hook=json_decode_object_hook)
    
    host_files = {}
    
    for chunk in index:
        assert isinstance(chunk, SW2_ConcreteReference)
        for netloc in chunk.location_hints:
            netloc = netloc[:netloc.find(':')]
            try:
                host_files[netloc].add(chunk.id)
            except KeyError:
                id_set = set()
                id_set.add(chunk.id)
                host_files[netloc] = id_set
                
    print host_files
    
    try:
        output_dir = sys.argv[1]
    except:
        output_dir = os.getcwd()
        
    for netloc, id_set in host_files.items():
        with open(os.path.join(output_dir, netloc), "w") as f:
            f.write('{%s}' % ','.join(list(id_set)))
    
if __name__ == '__main__':
    main()