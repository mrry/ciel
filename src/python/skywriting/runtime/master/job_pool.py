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
from skywriting.runtime.task import TASK_STATES,\
    build_taskpool_task_from_descriptor
from threading import Lock, Condition
import uuid
from cherrypy.process import plugins
import os
import simplejson
from skywriting.runtime.block_store import SWReferenceJSONEncoder
import struct
import logging
import cherrypy

JOB_ACTIVE = 0
JOB_COMPLETED = 1
JOB_FAILED = 2

JOB_STATES = {'ACTIVE': JOB_ACTIVE,
              'COMPLETED': JOB_COMPLETED,
              'FAILED': JOB_FAILED}

JOB_STATE_NAMES = {}
for (name, number) in JOB_STATES.items():
    JOB_STATE_NAMES[number] = name

RECORD_HEADER_STRUCT = struct.Struct('!cI')

class Job:
    
    def __init__(self, id, root_task, job_dir=None):
        self.id = id
        self.root_task = root_task
        self.job_dir = job_dir
        
        self.state = JOB_ACTIVE
        
        self.result_ref = None

        self.task_journal_fp = None
        
        # Counters for each task state.
        self.task_state_counts = {}
        for state in TASK_STATES.values():
            self.task_state_counts[state] = 0
        self._lock = Lock()
        self._condition = Condition(self._lock)

    def completed(self, result_ref):
        cherrypy.log.error('Job %s completed' % self.id, 'JOB', logging.INFO)
        self.state = JOB_COMPLETED
        self.result_ref = result_ref
        with self._lock:
            self._condition.notify_all()
        self.stop_journalling()
            
    def failed(self):
        self.state = JOB_FAILED
        with self._lock:
            self._condition.notify_all()

    def start_journalling(self):
        if self.task_journal_fp is None and self.job_dir is not None:
            self.task_journal_fp = open(os.path.join(self.job_dir, 'task_journal'), 'ab')

    def stop_journalling(self):
        with self._lock:
            if self.task_journal_fp is not None:
                self.task_journal_fp.close()
            self.task_journal_fp = None
                
        if self.job_dir is not None:
            with open(os.path.join(self.job_dir, 'result'), 'w') as result_file:
                simplejson.dump(self.result_ref, result_file, cls=SWReferenceJSONEncoder)

    def flush_journal(self):
        with self._lock:
            if self.task_journal_fp is not None:
                self.task_journal_fp.flush()
                os.fsync(self.task_journal_fp.fileno())

    def add_reference(self, id, ref, should_sync=False):
        with self._lock:
            if self.task_journal_fp is not None:
                ref_details = simplejson.dumps({'id': id, 'ref': ref}, cls=SWReferenceJSONEncoder)
                self.task_journal_fp.write(RECORD_HEADER_STRUCT.pack('R', len(ref_details)))
                self.task_journal_fp.write(ref_details)
                if should_sync:
                    self.task_journal_fp.flush()
                    os.fsync(self.task_journal_fp.fileno())

    def add_task(self, task, should_sync=False):
        with self._lock:
            self.task_state_counts[task.state] = self.task_state_counts[task.state] + 1
            if self.task_journal_fp is not None:
                task_details = simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder)
                self.task_journal_fp.write(RECORD_HEADER_STRUCT.pack('T', len(task_details)))
                self.task_journal_fp.write(task_details)
                if should_sync:
                    self.task_journal_fp.flush()
                    os.fsync(self.task_journal_fp.fileno())

    def record_state_change(self, prev_state, next_state):
        with self._lock:
            self.task_state_counts[prev_state] = self.task_state_counts[prev_state] - 1
            self.task_state_counts[next_state] = self.task_state_counts[next_state] + 1

    def as_descriptor(self):
        counts = {}
        ret = {'job_id': self.id, 
               'task_counts': counts, 
               'state': JOB_STATE_NAMES[self.state], 
               'root_task': self.root_task.task_id if self.root_task is not None else None,
               'expected_outputs': self.root_task.expected_outputs if self.root_task is not None else None,
               'result_ref': self.result_ref}
        with self._lock:
            for (name, state_index) in TASK_STATES.items():
                counts[name] = self.task_state_counts[state_index]
        return ret

class JobPool(plugins.SimplePlugin):

    def __init__(self, bus, task_pool, journal_root, global_name_directory):
        plugins.SimplePlugin.__init__(self, bus)
        self.task_pool = task_pool
        self.journal_root = journal_root
        self.global_name_directory = global_name_directory
    
        # Mapping from job ID to job object.
        self.jobs = {}
        
        # Synchronisation code for stopping/waiters.
        self.is_stopping = False
        self.current_waiters = 0
        self.max_concurrent_waiters = 10
    
    def subscribe(self):
        # Higher priority than the HTTP server
        self.bus.subscribe("stop", self.server_stopping, 10)

    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.server_stopping)
        
    def start_all_jobs(self):
        for job in self.jobs.values():
            self.start_job(job)
        
    def server_stopping(self):
        # When the server is shutting down, we need to notify all threads
        # waiting on job completion.
        self.is_stopping = True
        for job in self.jobs.values():
            with job._lock:
                job._condition.notify_all()
        
    def get_job_by_id(self, id):
        return self.jobs[id]
    
    def get_all_job_ids(self):
        return self.jobs.keys()
    
    def allocate_job_id(self):
        return str(uuid.uuid1())
    
    def add_job(self, job, sync_journal=False):
        # We will use this both for new jobs and on recovery.
        self.jobs[job.id] = job
    
    def add_failed_job(self, job_id):
        job = Job(job_id, None, None)
        job.state = JOB_FAILED
        self.jobs[job_id] = job
    
    def create_job_for_task(self, task_descriptor, job_id=None):
        
        if job_id is None:
            job_id = self.allocate_job_id()
        task_id = 'root:%s' % (job_id, )

        # TODO: Here is where we will set up the job journal, etc.
        job_dir = self.make_job_directory(job_id)
        
        # TODO: Remove the global name directory dependency.
        try:
            expected_outputs = task_descriptor['expected_outputs']
            for output in expected_outputs:
                self.global_name_directory.create_global_id(task_id, output)
        except KeyError:
            try:
                num_outputs = task_descriptor['num_outputs']
                expected_outputs = map(lambda x: self.global_name_directory.create_global_id(task_id), range(0, num_outputs))
            except:
                expected_outputs = [self.global_name_directory.create_global_id()]
            task_descriptor['expected_outputs'] = expected_outputs
            
        task = build_taskpool_task_from_descriptor(task_id, task_descriptor, self, None)
        job = Job(job_id, task, job_dir)
        task.job = job
        
        self.add_job(job)
        
        cherrypy.log('Added job: %s' % job.id, 'JOB_POOL', logging.INFO)

        return job

    def make_job_directory(self, job_id):
        if self.journal_root is not None:
            job_dir = os.path.join(self.journal_root, job_id)
            os.mkdir(job_dir)
            return job_dir
        else:
            return None

    def start_job(self, job):
        cherrypy.log('Starting job ID: %s' % job.id, 'JOB_POOL', logging.INFO)
        job.start_journalling()
        self.task_pool.add_task(job.root_task, True)
        
    def restart_job(self, job):
        job.start_journalling()
        self.task_pool.register_job_interest_for_output(job.root_task.expected_outputs[0], job)
        self.task_pool.do_graph_reduction(object_ids=job.root_task.expected_outputs)

    def wait_for_completion(self, job):
        with job._lock:
            while job.state == JOB_ACTIVE:
                if self.is_stopping:
                    break
                elif self.current_waiters > self.max_concurrent_waiters:
                    break
                else:
                    self.current_waiters += 1
                    job._condition.wait()
                    self.current_waiters -= 1
            if self.is_stopping:
                raise Exception("Server stopping")
            elif self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")
            else:
                return job
