'''
Created on 15 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from Queue import Queue
from cherrypy.process import plugins
from threading import Lock
from mrry.mercator.runtime.references import build_reference_from_tuple,\
    SWGlobalFutureReference, SWURLReference
import logging
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
    
    def __init__(self, task_id, task_descriptor, global_name_directory, parent_task_id=None):
        self.task_id = task_id
        self.handler = task_descriptor['handler']
        self.inputs = {}
        self.current_attempt = 0
        self.worker_id = None
        
        self.parent = parent_task_id
        self.children = []
        
        for local_id, input_tuple in task_descriptor['inputs'].items():
            self.inputs[local_id] = build_reference_from_tuple(input_tuple)
        
        self.expected_outputs = task_descriptor['expected_outputs']
    
        self.state = TASK_RUNNABLE    
    
        self.blocking_dict = {}
        for local_id, input in self.inputs.items():
            if isinstance(input, SWGlobalFutureReference):
                global_id = input.id
                urls = global_name_directory.get_urls_for_id(global_id)
                if len(urls) > 0:
                    self.inputs[local_id] = SWURLReference(urls)
                else:
                    try:
                        self.blocking_dict[global_id].add(local_id)
                    except KeyError:
                        self.blocking_dict[global_id] = set([local_id])

        if len(self.blocking_dict) > 0:
            self.state = TASK_BLOCKING
            
        
        # select()-handling code.
        try:
            tuple_select_group = task_descriptor['select_group']
            self.selecting_dict = {}
            self.select_result = []
            
            for i, ref_tuple in enumerate(tuple_select_group):
                ref = build_reference_from_tuple(ref_tuple)
                print "Selecting on ref:", ref
                if isinstance(ref, SWGlobalFutureReference):
                    global_id = ref.id
                    urls = global_name_directory.get_urls_for_id(global_id)
                    if len(urls) > 0:
                        self.select_result.append(i)
                    else:
                        self.selecting_dict[global_id] = i
                else:
                    self.select_result.append(i)

            if len(tuple_select_group) > 0 and len(self.select_result) == 0:
                self.state = TASK_SELECTING

            print "^^^!^^^ SELECT DICT IS", self.selecting_dict
            print "^^^!^^^ SELECT RESULT IS", self.select_result
            
        except KeyError:
            pass

        print "@@@ CREATED TASK %d in state %s" % (self.task_id, TASK_STATE_NAMES[self.state])
        
    def is_blocked(self):
        return self.state in (TASK_BLOCKING, TASK_SELECTING)
            
    def blocked_on(self):
        if self.state == TASK_SELECTING:
            return self.selecting_dict.keys()
        elif self.state == TASK_BLOCKING:
            return self.blocking_dict.keys()
        else:
            return []
    
    def unblock_on(self, global_id, urls):
        if self.state in (TASK_RUNNABLE, TASK_SELECTING):
            i = self.selecting_dict.pop(global_id)
            self.select_result.append(i)
            print "^^^!^^^ SELECT RESULT IS", self.select_result
            self.state = TASK_RUNNABLE
        elif self.state in (TASK_BLOCKING):
            local_ids = self.blocking_dict.pop(global_id)
            for local_id in local_ids:
                self.inputs[local_id] = SWURLReference(urls)
            if len(self.blocking_dict) == 0:
                self.state = TASK_RUNNABLE
        
    def as_descriptor(self):        
        tuple_inputs = {}
        for local_id, input in self.inputs.items():
            tuple_inputs[local_id] = input.as_tuple()
            
        descriptor = {'task_id': self.task_id,
                      'handler': self.handler,
                      'expected_outputs': self.expected_outputs,
                      'inputs': tuple_inputs,
                      'state': TASK_STATE_NAMES[self.state],
                      'parent': self.parent,
                      'children': self.children}
        
        if hasattr(self, 'select_result'):
            print "Adding select_result to descriptor:", self.select_result
            descriptor['select_result'] = self.select_result
        
        return descriptor
        
class TaskPool(plugins.SimplePlugin):
    
    def __init__(self, bus, global_name_directory):
        plugins.SimplePlugin.__init__(self, bus)
        self.global_name_directory = global_name_directory
        self.current_task_id = 0
        self.tasks = {}
        self.runnable_queue = Queue()
        self.references_blocking_tasks = {}
        self._lock = Lock()
    
    def subscribe(self):
        self.bus.subscribe('global_name_available', self.reference_available)
        self.bus.subscribe('task_failed', self.task_failed)
    
    def unsubscribe(self):
        self.bus.unsubscribe('global_name_available', self.reference_available)
        self.bus.unsubscribe('task_failed', self.task_failed)
    
    def add_task(self, task_descriptor, parent_task_id=None):
        with self._lock:
            task_id = self.current_task_id
            self.current_task_id += 1
            
            task = Task(task_id, task_descriptor, self.global_name_directory, parent_task_id)
            self.tasks[task_id] = task
        
            print "/// CREATED TASK:", task_id
        
            if task.is_blocked():
                print "/// TASK", task_id, "IS BLOCKED"
                for global_id in task.blocked_on():
                    try:
                        self.references_blocking_tasks[global_id].add(task_id)
                    except KeyError:
                        self.references_blocking_tasks[global_id] = set([task_id])
            else:
                print "/// TASK", task_id, "IS RUNNABLE"
                self.runnable_queue.put(task)
                
        self.bus.publish('schedule')
        return task
    
    def mark_task_as_aborted(self, task_id):
        with self._lock:
            task = self.tasks[task_id]
            previous_state = task.state
            task.state = TASK_ABORTED
            return task, previous_state
    
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
                    print "/// TASK", task_id, "IS NOW RUNNABLE"
                    self.runnable_queue.put(task)
    
    def get_task_by_id(self, id):
        return self.tasks[id]
    
    def task_completed(self, id):
        with self._lock:
            task = self.tasks[id]
            worker_id = task.worker_id
            task.worker_id = None
            task.state = TASK_COMMITTED
            
        self.bus.publish('worker_idle', worker_id)
    
    def task_failed(self, id, reason, details=None):
        cherrypy.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        if reason == 'WORKER_FAILED':
            # Try to reschedule task.
            with self._lock:
                task = self.tasks[id]
                task.current_attempt += 1
                task.worker_id = None
                if task.current_attempt > 3:
                    task.state = TASK_FAILED
                    # TODO: notify parents.
                else:
                    task.state = TASK_RUNNABLE
                    self.runnable_queue.put(task)
        elif reason == 'MISSING_INPUT':
            # Problem fetching input, so we will have to recreate it.
            pass
        elif reason == 'RUNTIME_EXCEPTION':
            # Kill the entire job, citing the problem.
            with self._lock:
                task = self.tasks[id]
                worker_id = task.worker_id
                task.worker_id = None
                task.state = TASK_FAILED
                # TODO: notify parents. 
            self.bus.publish('worker_idle', worker_id)
            pass
