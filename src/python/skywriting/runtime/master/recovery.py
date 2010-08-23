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
from skywriting.runtime.references import SW2_ConcreteReference, SWNoProvenance,\
    ACCESS_SWBS
from cherrypy.process import plugins
import urllib2
from skywriting.runtime.block_store import BLOCK_LIST_RECORD_STRUCT
import logging
import cherrypy

class RecoveryManager(plugins.SimplePlugin):
    
    def __init__(self, bus, task_pool, deferred_worker):
        plugins.SimplePlugin.__init__(self, bus)
        self.task_pool = task_pool
        self.deferred_worker = deferred_worker
        
    def subscribe(self):
        self.bus.subscribe('fetch_block_list', self.fetch_block_list_defer)
    
    def unsubscribe(self):
        self.bus.subscribe('fetch_block_list', self.fetch_block_list_defer)

    def fetch_block_list_defer(self, worker):
        print 'In fetch_block_list_defer'
        self.deferred_worker.do_deferred(lambda: self.fetch_block_names_from_worker(worker))
        
    def fetch_block_names_from_worker(self, worker):
        '''
        Loop through the block list file from the given worker, and publish the
        references found there.
        '''
        
        print 'In fetch_block_names_from_worker(%s)' % str(worker)
        
        block_file = urllib2.urlopen('http://%s/data/' % worker.netloc)

        while True:
            record = block_file.read(BLOCK_LIST_RECORD_STRUCT.size)
            if not record:
                break
            block_name, block_size = BLOCK_LIST_RECORD_STRUCT.unpack(record)
            conc_ref = SW2_ConcreteReference(block_name, SWNoProvenance(), 
                                             block_size)
            conc_ref.add_location_hint(worker.netloc, ACCESS_SWBS)
            
            cherrypy.log.error('Recovering block %s (size=%d)' % (block_name, block_size), 'RECOVERY', logging.INFO)
            self.task_pool.publish_single_ref(block_name, conc_ref)

        
        block_file.close()
        