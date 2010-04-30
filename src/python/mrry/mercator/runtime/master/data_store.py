'''
Created on Apr 15, 2010

@author: derek
'''
from __future__ import with_statement
from threading import Lock
import cherrypy

class GlobalNameDirectory:
    
    def __init__(self):
        self._lock = Lock()
        self.current_id = 0
        self.name_bindings = {}
        self.task_mapping = {}
    
    def create_global_id(self, task_id=None, refs=[]):
        ref_set = set()
        with self._lock:
            id = self.current_id
            self.current_id += 1
            self.name_bindings[id] = ref_set
            self.task_mapping[id] = task_id
        for ref in refs:
            ref_set.add(ref)
        return id
    
    def get_task_for_id(self, id):
        return self.task_mapping[id]
    
    def set_task_for_id(self, id, task_id):
        self.task_mapping[id] = task_id
    
    def add_refs_for_id(self, id, refs):
        with self._lock:
            ref_set = self.name_bindings[id]
            for ref in refs:
                print "Adding ref:", ref
                ref_set.add(ref)
            ref_list = list(ref_set)
        cherrypy.engine.publish('global_name_available', id, ref_list)
    
    def get_refs_for_id(self, id):
        with self._lock:
            url_set = self.name_bindings[id]
            return list(url_set)
