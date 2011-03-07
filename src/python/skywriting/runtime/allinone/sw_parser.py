# Copyright (c) 2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from skywriting.lang.context import SimpleContext
from skywriting.lang.parser import SWScriptParser
from skywriting.runtime.executors import SWContinuation
import os
import sys

def build_initial_task_descriptor(filename, block_store, task_name='root', cont_id='root_cont', output_id='root_output', env=False):

    parser = SWScriptParser()

    with open(filename, 'r') as script_file:
        script = parser.parse(script_file.read())

    if script is None:
        print >>sys.stderr, 'Error parsing script in file: %s' % filename
        exit(-1)
    
    cont = SWContinuation(script, SimpleContext())
    if env:
        cont.context.bind_identifier('env', os.environ)
       
    cont_id = 'root_cont'
    cont_ref = block_store.ref_from_object(cont, 'pickle', cont_id)
    print cont_ref
    #cont_ref = SW2_ConcreteReference(cont_id, cont_len, [block_store.netloc])
    expected_output_id = 'root_output'
    
    tp_ref = block_store.ref_from_object({'cont' : cont_ref}, 'pickle', cont_id + ':tp')
    
    task_descriptor = {'task_id' : 'root', 'dependencies': [cont_ref], 'task_private': tp_ref, 'handler': 'swi', 'expected_outputs' : [expected_output_id]}
    
    return task_descriptor, cont_ref
    
