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
'''
Created on 15 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from cherrypy.process import plugins
from threading import Lock, Condition
from skywriting.runtime.references import \
    SWURLReference, SWErrorReference, SW2_FutureReference, SW2_ConcreteReference
from skywriting.runtime.block_store import get_netloc_for_sw_url
import time
import datetime
import logging
import uuid
import cherrypy

TASK_CREATED = -1
TASK_BLOCKING = 0
TASK_SELECTING = 1
TASK_RUNNABLE = 2
TASK_QUEUED = 3
TASK_ASSIGNED = 4
TASK_COMMITTED = 5
TASK_FAILED = 6
TASK_ABORTED = 7

TASK_STATES = {'CREATED': TASK_CREATED,
               'BLOCKING': TASK_BLOCKING,
               'SELECTING': TASK_SELECTING,
               'RUNNABLE': TASK_RUNNABLE,
               'QUEUED': TASK_QUEUED,
               'ASSIGNED': TASK_ASSIGNED,
               'COMMITTED': TASK_COMMITTED,
               'FAILED': TASK_FAILED,
               'ABORTED': TASK_ABORTED}

TASK_STATE_NAMES = {}
for (name, number) in TASK_STATES.items():
    TASK_STATE_NAMES[number] = name

class Task:
    
    def __init__(self, task_id, task_descriptor, global_name_directory, task_pool, parent_task_id=None):
        self.task_id = task_id
        self.handler = task_descriptor['handler']
        self.inputs = {}
        self.current_attempt = 0
        self.worker_id = None
        self.event_index = 0
        self.task_pool = task_pool
        
        self.history = []
       
        self.parent = parent_task_id
        self.children = []
        
        self.dependencies = task_descriptor['inputs']
        
        self.inputs = {}
                
        self.expected_outputs = task_descriptor['expected_outputs']
    
        try:
            self.save_continuation = task_descriptor['save_continuation']
        except KeyError:
            self.save_continuation = False

        try:
            self.continues_task = uuid.UUID(hex=task_descriptor['continues_task'])
        except KeyError:
            self.continues_task = None

        self.saved_continuation_uri = None
    
        self.state = TASK_RUNNABLE    
    
        self.blocking_dict = {}
        for local_id, input in self.dependencies.items():
            if isinstance(input, SW2_FutureReference):
                global_id = input.id
                refs = global_name_directory.get_refs_for_id(global_id)
                if len(refs) > 0:
                    self.inputs[local_id] = refs[0]
                else:
                    try:
                        self.blocking_dict[global_id].add(local_id)
                    except KeyError:
                        self.blocking_dict[global_id] = set([local_id])
            else:
                self.inputs[local_id] = input

        if len(self.blocking_dict) > 0:
            self.state = TASK_BLOCKING
            
        
        # select()-handling code.
        try:
            select_group = task_descriptor['select_group']
            self.selecting_dict = {}
            self.select_result = []
            
            for i, ref in enumerate(select_group):
                if isinstance(ref, SW2_FutureReference):
                    global_id = ref.id
                    refs = global_name_directory.get_refs_for_id(global_id)
                    if len(refs) > 0:
                        self.select_result.append(i)
                    else:
                        self.selecting_dict[global_id] = i
                else:
                    self.select_result.append(i)

            if len(select_group) > 0 and len(self.select_result) == 0:
                self.state = TASK_SELECTING
            
        except KeyError:
            pass
        
        self.record_event("CREATED")

    # Warning: called under worker_pool._lock
    def set_assigned_to_worker(self, worker_id):
        self.worker_id = worker_id
        self.state = TASK_ASSIGNED
        self.record_event("ASSIGNED")
        self.task_pool.notify_task_assigned_to_worker_id(self, worker_id)
        
    def __repr__(self):
        return 'Task(%d)' % self.task_id
       
    def record_event(self, description):
        self.history.append((datetime.datetime.now(), description))
        
    def is_blocked(self):
        return self.state in (TASK_BLOCKING, TASK_SELECTING)
            
    def blocked_on(self):
        if self.state == TASK_SELECTING:
            return self.selecting_dict.keys()
        elif self.state == TASK_BLOCKING:
            return self.blocking_dict.keys()
        else:
            return []
    
    def unblock_on(self, global_id, refs):
        if self.state in (TASK_RUNNABLE, TASK_SELECTING):
            i = self.selecting_dict.pop(global_id)
            self.select_result.append(i)
            self.state = TASK_RUNNABLE
            self.record_event("RUNNABLE")
        elif self.state in (TASK_BLOCKING,):
            local_ids = self.blocking_dict.pop(global_id)
            for local_id in local_ids:
                self.inputs[local_id] = refs[0]
            if len(self.blocking_dict) == 0:
                self.state = TASK_RUNNABLE
                self.record_event("RUNNABLE")
        
    def as_descriptor(self, long=False):        
        descriptor = {'task_id': str(self.task_id),
                      'dependencies': self.dependencies,
                      'handler': self.handler,
                      'expected_outputs': map(str, self.expected_outputs),
                      'inputs': self.inputs,
                      'event_index': self.event_index}
        
        if long:
            descriptor['history'] = map(lambda (t, name): (time.mktime(t.timetuple()) + t.microsecond / 1e6, name), self.history)
            descriptor['worker_id'] = self.worker_id
            descriptor['saved_continuation_uri'] = self.saved_continuation_uri
            descriptor['state'] = TASK_STATE_NAMES[self.state]
            descriptor['parent'] = self.parent
            descriptor['children'] = self.children
                    
        if hasattr(self, 'select_result'):
            descriptor['select_result'] = self.select_result
        
        if self.save_continuation:
            descriptor['save_continuation'] = True
        if self.continues_task:
            descriptor['continues_task'] = str(self.continues_task)
        
        return descriptor        

class TaskPool(plugins.SimplePlugin):
    
    def __init__(self, bus, global_name_directory, worker_pool):
        plugins.SimplePlugin.__init__(self, bus)
        self.global_name_directory = global_name_directory
        self.worker_pool = worker_pool
        self.current_task_id = 0
        self.tasks = {}
        self.references_blocking_tasks = {}
        self._lock = Lock()
        self._cond = Condition(self._lock)
        self.max_concurrent_waiters = 5
        self.current_waiters = 0
        self.event_index = 0
        # event_index: The index which will be given to the *next* event
        self.events = []
        self.is_stopping = False
    
    # Call under _lock (and don't release _lock until you've put an event in the queue!)
    def new_event(self, t):
        ret = dict()
        ret["index"] = self.event_index
        ret["task_id"] = t.task_id
        t.event_index = self.event_index
        self.event_index += 1
        self._cond.notify_all()
        return ret

    def wait_event_after(self, idx):
        with self._lock:
            self.current_waiters = self.current_waiters + 1
            while idx == self.event_index:
                if self.current_waiters > self.max_concurrent_waiters:
                    break
                elif self.is_stopping:
                    break
                else:
                    self._cond.wait()
            self.current_waiters = self.current_waiters - 1
            if self.is_stopping:
                raise Exception("Server stopping")
            elif self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")
            
    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            self._cond.notify_all()

    def subscribe(self):
        self.bus.subscribe('global_name_available', self.reference_available)
        self.bus.subscribe('task_failed', self.task_failed)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        # Stop method gets run before the HTTP server
    
    def unsubscribe(self):
        self.bus.unsubscribe('global_name_available', self.reference_available)
        self.bus.unsubscribe('task_failed', self.task_failed)
        self.bus.unsubscribe('stop', self.server_stopping)
    
    def compute_best_worker_for_task(self, task):
        netlocs = {}
        for input in task.inputs.values():
            if isinstance(input, SWURLReference) and input.size_hint is not None:
                for url in input.urls:
                    netloc = get_netloc_for_sw_url(url)
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
            elif isinstance(input, SW2_ConcreteReference) and input.size_hint is not None:
                for netloc in input.location_hints.keys():
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
        ranked_netlocs = [(saving, netloc) for (netloc, saving) in netlocs.items()]
        if len(ranked_netlocs) > 0:
            return self.worker_pool.get_worker_at_netloc(min(ranked_netlocs)[1])
        else:
            return None
    
    def add_task_to_queues(self, task):
        # TODO: Compute best worker(s) here.
        best_worker = self.compute_best_worker_for_task(task)
        task.state = TASK_QUEUED
        if best_worker is not None:
            best_worker.local_queue.put(task)
        handler_queue = self.worker_pool.feature_queues.get_queue_for_feature(task.handler)
        handler_queue.put(task)
    
    def generate_task_id(self):
        return uuid.uuid1()
    
    def add_task(self, task_descriptor, parent_task_id=None):
        with self._lock:
            try:
                task_id = uuid.UUID(hex=task_descriptor['task_id']) 
            except:
                task_id = self.generate_task_id()
            
            task = Task(task_id, task_descriptor, self.global_name_directory, self, parent_task_id)
            self.tasks[task_id] = task
            add_event = self.new_event(task)
            add_event["task_descriptor"] = task.as_descriptor(long=True)
            add_event["action"] = "CREATED"
        
            if task.is_blocked():
                for global_id in task.blocked_on():
                    try:
                        self.references_blocking_tasks[global_id].add(task_id)
                    except KeyError:
                        self.references_blocking_tasks[global_id] = set([task_id])
            else:
                task.state = TASK_RUNNABLE
                self.add_task_to_queues(task)

            self.events.append(add_event)
                
        self.bus.publish('schedule')
        return task

    # Warning: called under worker_pool._lock
    def notify_task_assigned_to_worker_id(self, task, worker_id):
        with self._lock:
            assigned_event = self.new_event(task)
            assigned_event["action"] = "ASSIGNED"
            assigned_event["worker_id"] = worker_id
            self.events.append(assigned_event)

    def _mark_task_as_aborted(self, task_id):
        task = self.tasks[task_id]
        previous_state = task.state
        task.state = TASK_ABORTED
        with self._lock:
            abort_event = self.new_event(task)
            abort_event["action"] = "ABORTED"
            self.events.append(abort_event)
        return task, previous_state

    def _abort(self, task_id):
        task, previous_state = self._mark_task_as_aborted(task_id)
        if previous_state == TASK_ASSIGNED:
            task.record_event("ABORTED")
            self.worker_pool.abort_task_on_worker(task)
        for child in task.children:
            self._abort(child)
        
    def abort(self, task_id):
        with self._lock:
            self._abort(task_id)
            
    def reference_available(self, id, urls):

        with self._lock:

            try:
                blocked_tasks_set = self.references_blocking_tasks.pop(id)
            except KeyError:
                return
            for task_id in blocked_tasks_set:
                task = self.tasks[task_id]
                was_blocked = task.is_blocked()
                task.unblock_on(id, urls)
                if was_blocked and not task.is_blocked():

                    runnable_event = self.new_event(task)
                    runnable_event["action"] = "RUNNABLE"
                    self.events.append(runnable_event)
                    task.state = TASK_RUNNABLE
                    task.record_event("RUNNABLE")
                    self.add_task_to_queues(task)
                    self.bus.publish('schedule')
                    
    def get_task_by_id(self, id):
        return self.tasks[id]
    
    def task_completed(self, id):
        
        with self._lock:
            task = self.tasks[id]
            worker_id = task.worker_id
            committed_event = self.new_event(task)
            committed_event["action"] = "COMMITTED"
            self.events.append(committed_event)
            task.state = TASK_COMMITTED
            task.record_event("COMMITTED")
            
        self.bus.publish('worker_idle', worker_id)
    
    def task_failed(self, id, reason, details=None):
        cherrypy.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        worker_id = None
        should_notify_outputs = False
        task = None

        with self._lock:
            task = self.tasks[id]
            failure_event = self.new_event(task)
            if reason == 'WORKER_FAILED':
                # Try to reschedule task.
                task.current_attempt += 1
                task.record_event("WORKER_FAILURE")
                if task.current_attempt > 3:
                    task.state = TASK_FAILED
                    task.record_event("TASK_FAILURE")
                    failure_event["action"] = "WORKER_FAIL"
                    # TODO: notify parents.
                else:
                    self.add_task_to_queues(task)
                    self.bus.publish('schedule')
                    failure_event["action"] = "WORKER_FAIL_RETRY"
            elif reason == 'MISSING_INPUT':
                # Problem fetching input, so we will have to rete it.
                task.record_event("MISSING_INPUT_FAILURE")
                failure_event["action"] = "MISSING_INPUT_FAIL"
            elif reason == 'RUNTIME_EXCEPTION':
                # Kill the entire job, citing the problem.
                worker_id = task.worker_id
                task.record_event("RUNTIME_EXCEPTION_FAILURE")
                task.state = TASK_FAILED
                failure_event["action"] = "RUNTIME_EXCEPTION_FAIL"
                # TODO: notify parents.
                should_notify_outputs = True
            
            self.events.append(failure_event)

        # Doing this outside the lock because this leads via add_refs_to_id
        # --> self::reference_available, creating a circular wait. We noted the task as FAILED inside the lock,
        # which ought to be enough.
        if should_notify_outputs:
            for output in task.expected_outputs:
                self.global_name_directory.add_refs_for_id(output, [SWErrorReference(reason, details)]) 

        if worker_id is not None:
            self.bus.publish('worker_idle', worker_id)

    def flush_task_dict(self):
        cherrypy.log.error("Flushing tasks dict. In-progress jobs will fail.", "TASK", logging.WARN, False)
        self.tasks = {}
        self.references_blocking_tasks = {}
