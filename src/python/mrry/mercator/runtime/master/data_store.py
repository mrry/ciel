'''
Created on Apr 15, 2010

@author: derek
'''
from __future__ import with_statement
from threading import Lock, Condition
import cherrypy

class GlobalNameDirectoryEntry:
    def __init__(self, task_id, refs):
        self.refs = refs
        self.task_id = task_id
        self.waiters = []
        
class GlobalNameDirectory:
    
    def __init__(self):
        self._lock = Lock()
        self.current_id = 0
        self.directory = {}
    
    def create_global_id(self, task_id=None):
        entry = GlobalNameDirectoryEntry(task_id, [])
        with self._lock:
            id = self.current_id
            self.current_id += 1
            self.directory[id] = entry
        return id
    
    def get_task_for_id(self, id):
        return self.directory[id].task_id
    
    def set_task_for_id(self, id, task_id):
        self.directory[id].task_id = task_id
    
    def add_refs_for_id(self, id, refs):
        with self._lock:
            entry = self.directory[id]
            for ref in refs:
                entry.refs.append(ref)
            for waiter in entry.waiters:
                waiter.notify()
        cherrypy.engine.publish('global_name_available', id, entry.refs)
    
    def get_refs_for_id(self, id):
        with self._lock:
            return self.directory[id].refs
        
    def wait_for_completion(self, id):
        with self._lock:
            entry = self.directory[id]
            if len(entry.refs) == 0:
                cond = Condition(self._lock)
                entry.waiters.append(cond)
                cond.wait()
        return entry.refs
