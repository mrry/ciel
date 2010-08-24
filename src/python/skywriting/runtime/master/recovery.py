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
from skywriting.runtime.block_store import BLOCK_LIST_RECORD_STRUCT,\
    json_decode_object_hook
import logging
import cherrypy
import os
import simplejson
from skywriting.runtime.master.job_pool import TASK_DESCRIPTOR_LENGTH_STRUCT,\
    Job
from skywriting.runtime.task import build_taskpool_task_from_descriptor

class RecoveryManager(plugins.SimplePlugin):
    
    def __init__(self, bus, job_pool, task_pool, deferred_worker):
        plugins.SimplePlugin.__init__(self, bus)
        self.job_pool = job_pool
        self.task_pool = task_pool
        self.deferred_worker = deferred_worker
        
    def subscribe(self):
        # In order to present a consistent view to clients, we must do this
        # before starting the webserver.
        self.bus.subscribe('start', self.recover_job_descriptors, 10)
        
        self.bus.subscribe('fetch_block_list', self.fetch_block_list_defer)
    
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.recover_job_descriptors)
        self.bus.unsubscribe('fetch_block_list', self.fetch_block_list_defer)

    def recover_job_descriptors(self):
        root = self.job_pool.journal_root
        if root is None:
            return
        
        for job_id in os.listdir(root):

            try:
                job_dir = os.path.join(root, job_id)
                result_path = os.path.join(job_dir, 'result')
                if os.path.exists(result_path):
                    with open(result_path, 'r') as result_file:
                        result = simplejson.load(result_file, object_hook=json_decode_object_hook)
                else:
                    result = None
                    
                journal_path = os.path.join(job_dir, 'task_journal')
                journal_file = open(journal_path, 'rb')
                root_task_descriptor_length, = TASK_DESCRIPTOR_LENGTH_STRUCT.unpack(journal_file.read(TASK_DESCRIPTOR_LENGTH_STRUCT.size))
                root_task_descriptor_string = journal_file.read(root_task_descriptor_length)
                assert len(root_task_descriptor_string) == root_task_descriptor_length
                root_task_descriptor = simplejson.loads(root_task_descriptor_string, object_hook=json_decode_object_hook)
                root_task_id = root_task_descriptor['task_id']
                root_task = build_taskpool_task_from_descriptor(root_task_id, root_task_descriptor, self.task_pool, None)
                job = Job(job_id, root_task, job_dir)
                if result is not None:
                    job.completed(result)
                self.job_pool.add_job(job)
                
                self.load_other_tasks_defer(job, journal_file)
                
            except:
                # We have lost critical data for the job, so we must fail it.
                cherrypy.log.error('Error recovering job %s' % job_id, 'RECOVERY', logging.ERROR, True)
                self.job_pool.add_failed_job(job_id)

    def load_other_tasks_defer(self, job, journal_file):
        self.deferred_worker.do_deferred(lambda: self.load_other_tasks_for_job(job, journal_file))

    def load_other_tasks_for_job(self, job, journal_file):
        '''
        Process a the task journal for a recovered job.
        '''
        try:
            while True:
                td_length, = TASK_DESCRIPTOR_LENGTH_STRUCT.unpack(journal_file.read(TASK_DESCRIPTOR_LENGTH_STRUCT.size))
                if td_length != TASK_DESCRIPTOR_LENGTH_STRUCT.size:
                    break
                td_string = journal_file.read(td_length)
                if len(td_string) != td_length:
                    break
                td = simplejson.loads(td_string, object_jook=json_decode_object_hook)
                task_id = td['task_id']
                parent_task = self.task_pool.get_task_by_id(td['parent'])
                task = build_taskpool_task_from_descriptor(task_id, td, self.task_pool, parent_task)
                task.job = job
                task.parent.children.append(task)
                self.task_pool.add_task(task)

        except:
            cherrypy.log.error('Error recovering task_journal for job %s' % job.id, 'RECOVERY', logging.WARNING, True)

        finally:
            journal_file.close()

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
        