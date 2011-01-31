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
import sys
from skywriting.runtime.allinone.sw_parser import build_initial_task_descriptor
from skywriting.runtime.block_store import BlockStore
import cherrypy
import os
import tempfile
from skywriting.runtime.task import TaskPoolTask,\
    build_taskpool_task_from_descriptor
from skywriting.runtime.task_graph import DynamicTaskGraph
import threading
from skywriting.runtime.allinone.task_runner import TaskRunner
import ciel
import logging


def main():
    
    if len(sys.argv) < 2:
        print >>sys.stderr, 'Usage: sw-allinone SCRIPT_FILENAME'
    script_filename = sys.argv[1]
    
    base_dir = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/sw-files-'))
    ciel.log('Writing block store files to %s' % base_dir, 'ALLINONE', logging.INFO)
    block_store = BlockStore(ciel.engine, 'localhost', 8000, base_dir, True)
    
    initial_task_descriptor, cont_ref = build_initial_task_descriptor(script_filename, block_store, 'root', 'root_cont', 'root_output')
        
    initial_task_object = build_taskpool_task_from_descriptor('root', initial_task_descriptor, None, None)
    
    task_runner = TaskRunner(initial_task_object, cont_ref, block_store)
    
    try:
        result = task_runner.run()
        
        print block_store.retrieve_object_for_ref(result, 'json')
        
    except:
        pass
    
    block_store.stop_thread()

if __name__ == '__main__':
    main()