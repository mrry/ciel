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
from cherrypy.process import plugins
from skywriting.runtime.block_store import SWReferenceJSONEncoder
from skywriting.runtime.task import TASK_STATES, \
    build_taskpool_task_from_descriptor
from threading import Lock, Condition
import Queue
import ciel
import datetime
import logging
import os
import simplejson
import skywriting.runtime.executors
import struct
import time
import uuid

JOB_CREATED = -1
JOB_ACTIVE = 0
JOB_COMPLETED = 1
JOB_FAILED = 2
JOB_QUEUED = 3
JOB_RECOVERED = 4
JOB_CANCELLED = 5

JOB_STATES = {'CREATED': JOB_CREATED,
              'ACTIVE': JOB_ACTIVE,
              'COMPLETED': JOB_COMPLETED,
              'FAILED': JOB_FAILED,
              'QUEUED': JOB_QUEUED,
              'RECOVERED': JOB_RECOVERED,
              'CANCELLED' : JOB_CANCELLED}

JOB_STATE_NAMES = {}
for (name, number) in JOB_STATES.items():
    JOB_STATE_NAMES[number] = name

RECORD_HEADER_STRUCT = struct.Struct('!cI')

class Job:
    
    def __init__(self, id, root_task, job_dir=None, state=JOB_CREATED, job_pool=None):
        self.id = id
        self.root_task = root_task
        self.job_dir = job_dir
        
        self.job_pool = job_pool
        
        self.history = []
        
        self.state = state
        
        self.result_ref = None

        self.task_journal_fp = None
        
        # Counters for each task state.
        self.task_state_counts = {}
        for state in TASK_STATES.values():
            self.task_state_counts[state] = 0
        self._lock = Lock()
        self._condition = Condition(self._lock)

        
    def record_event(self, description):
        self.history.append((datetime.datetime.now(), description))
                    
    def set_state(self, state):
        self.record_event(JOB_STATE_NAMES[state])
        self.state = state
        evt_time = self.history[-1][0]
        ciel.log('%s %s @ %f' % (self.id, JOB_STATE_NAMES[self.state], time.mktime(evt_time.timetuple()) + evt_time.microsecond / 1e6), 'JOB', logging.INFO)
         
    def failed(self):
        self.set_state(JOB_FAILED)
        self.stop_journalling()
        with self._lock:
            self._condition.notify_all()

    def enqueued(self):
        self.set_state(JOB_QUEUED)

    def completed(self, result_ref):
        self.set_state(JOB_COMPLETED)
        self.result_ref = result_ref
        with self._lock:
            self._condition.notify_all()
        self.stop_journalling()        

    def activated(self):
        self.set_state(JOB_ACTIVE)
        if self.task_journal_fp is None and self.job_dir is not None:
            self.task_journal_fp = open(os.path.join(self.job_dir, 'task_journal'), 'ab')

    def cancelled(self):
        self.set_state(JOB_CANCELLED)
        self.stop_journalling()

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

    def __init__(self, bus, task_pool, journal_root):
        plugins.SimplePlugin.__init__(self, bus)
        self.task_pool = task_pool
        self.journal_root = journal_root
    
        # Mapping from job ID to job object.
        self.jobs = {}
        
        self.current_running_job = None
        self.run_queue = Queue.Queue()
        
        # Synchronisation code for stopping/waiters.
        self.is_stopping = False
        self.current_waiters = 0
        self.max_concurrent_waiters = 10
        
        self._lock = Lock()
    
    def subscribe(self):
        # Higher priority than the HTTP server
        self.bus.subscribe("stop", self.server_stopping, 10)

    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.server_stopping)
        
    def start_all_jobs(self):
        for job in self.jobs.values():
            self.queue_job(job)
        
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
        with self._lock:
            self.jobs[job.id] = job
        
        # We will use this both for new jobs and on recovery.
        if job.root_task is not None:
            self.task_pool.add_task(job.root_task, False)
    
    def add_failed_job(self, job_id):
        job = Job(job_id, None, None, JOB_FAILED, self)
        self.jobs[job_id] = job
    
    def create_job_for_task(self, task_descriptor, job_id=None):
        
        if job_id is None:
            job_id = self.allocate_job_id()
        task_id = 'root:%s' % (job_id, )

        # TODO: Here is where we will set up the job journal, etc.
        job_dir = self.make_job_directory(job_id)
        
        try:
            expected_outputs = task_descriptor['expected_outputs']
        except KeyError:
            expected_outputs = ['%s:job_output' % job_id]
            task_descriptor['expected_outputs'] = expected_outputs
            
        task = build_taskpool_task_from_descriptor(task_id, task_descriptor, self, None)
        job = Job(job_id, task, job_dir, JOB_CREATED, self)
        task.job = job
        
        self.add_job(job)
        
        ciel.log('Added job: %s' % job.id, 'JOB_POOL', logging.INFO)

        return job

    def make_job_directory(self, job_id):
        if self.journal_root is not None:
            job_dir = os.path.join(self.journal_root, job_id)
            os.mkdir(job_dir)
            return job_dir
        else:
            return None

    def queue_job(self, job):
        self.run_queue.put(job)
        job.enqueued()
        
        with self._lock:
            if self.current_running_job is None:
                next_job = self.run_queue.get()
                self._start_job(next_job)
            
    def job_completed(self, job, result_ref):
        assert job is self.current_running_job
        
        job.completed(result_ref)

        with self._lock:
            try:
                self.current_running_job = self.run_queue.get_nowait()
                self._start_job(self.current_running_job)
            except:
                self.current_running_job = None

    def _start_job(self, job):
        ciel.log('Starting job ID: %s' % job.id, 'JOB_POOL', logging.INFO)
        self.current_running_job = job 
        job.activated()
        self.task_pool.register_job_interest_for_output(job.root_task.expected_outputs[0], job)
        self.task_pool.do_graph_reduction(object_ids=job.root_task.expected_outputs)
        #self.task_pool.add_task(job.root_task, True)
        
#    def restart_job(self, job):
#        job.start()
#        self.task_pool.register_job_interest_for_output(job.root_task.expected_outputs[0], job)
#        self.task_pool.do_graph_reduction(object_ids=job.root_task.expected_outputs)

    def wait_for_completion(self, job):
        with job._lock:
            while job.state not in (JOB_COMPLETED, JOB_FAILED):
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
