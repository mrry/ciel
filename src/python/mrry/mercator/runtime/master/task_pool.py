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


class Task:
    
    def __init__(self, task_id, task_descriptor, global_name_directory):
        self.task_id = task_id
        self.handler = task_descriptor['handler']
        self.inputs = {}
        self.current_attempt = 0
        
        self.worker_id = None
        
        for local_id, input_tuple in task_descriptor['inputs'].items():
            self.inputs[local_id] = build_reference_from_tuple(input_tuple)
        
        self.expected_outputs = task_descriptor['expected_outputs']
    
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
        
    def is_blocked(self):
        return len(self.blocking_dict) > 0
    
    def blocked_on(self):    
        return self.blocking_dict.keys()
    
    def unblock_on(self, global_id, urls):
        local_ids = self.blocking_dict.pop(global_id)
        for local_id in local_ids:
            self.inputs[local_id] = SWURLReference(urls)
        
    def as_descriptor(self):        
        tuple_inputs = {}
        for local_id, input in self.inputs.items():
            tuple_inputs[local_id] = input.as_tuple()
        return {'task_id': self.task_id,
                'handler': self.handler,
                'expected_outputs': self.expected_outputs,
                'inputs': tuple_inputs}
        
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
    
    def add_task(self, task_descriptor):
        with self._lock:
            task_id = self.current_task_id
            self.current_task_id += 1
            
            task = Task(task_id, task_descriptor, self.global_name_directory)
            self.tasks[task_id] = task
        
            if task.is_blocked():
                for global_id in task.blocked_on():
                    try:
                        self.references_blocking_tasks[global_id].add(task_id)
                    except KeyError:
                        self.references_blocking_tasks[global_id] = set([task_id])
            else:
                self.runnable_queue.put(task)
                
        self.bus.publish('schedule')
    
    def reference_available(self, id, urls):
        with self._lock:
            try:
                blocked_tasks_set = self.references_blocking_tasks.pop(id)
            except KeyError:
                return
            for task_id in blocked_tasks_set:
                task = self.tasks[task_id]
                task.unblock_on(id, urls)
                if not task.is_blocked():
                    self.runnable_queue.put(task)
    
    def task_completed(self, id):
        with self._lock:
            task = self.tasks[id]
            worker_id = task.worker_id
            task.worker_id = None
            
        self.bus.publish('worker_idle', worker_id)
    
    def task_failed(self, id, reason, details=None):
        cherrypy.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        if reason == 'WORKER_FAILED':
            # Try to reschedule task.
            with self._lock:
                task = self.tasks[id]
                task.current_attempt += 1
                task.worker_id = None
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
            self.bus.publish('worker_idle', worker_id)
            pass
