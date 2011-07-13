# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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
import os
import sys

inputs = None
outputs = None

def init():
    global inputs, outputs
    
    try:
        input_files = os.getenv("INPUT_FILES")
        output_files = os.getenv("OUTPUT_FILES")
        
        if input_files is None or output_files is None:
            raise KeyError()
        
        with open(input_files) as f:
            inputs = [open(x.strip(), 'r') for x in f.readlines()]
            
        with open(output_files) as f:
            outputs = [open(x.strip(), 'w') for x in f.readlines()]
        
    except KeyError:
        print >>sys.stderr, "--- DEBUG MODE - using standard input and output ---"
        inputs = [sys.stdin]
        outputs = [sys.stdout]