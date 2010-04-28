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
    
    def create_global_id(self, task_id=None, urls=[]):
        url_set = set()
        with self._lock:
            id = self.current_id
            self.current_id += 1
            self.name_bindings[id] = url_set
            self.task_mapping[id] = task_id
        for url in urls:
            url_set.add(url)
        return id
    
    def get_task_for_id(self, id):
        return self.task_mapping[id]
    
    def set_task_for_id(self, id, task_id):
        self.task_mapping[id] = task_id
    
    def add_urls_for_id(self, id, urls):
        with self._lock:
            url_set = self.name_bindings[id]
            for url in urls:
                url_set.add(url)
            url_list = list(url_set)
        cherrypy.engine.publish('global_name_available', id, url_list)
    
    def get_urls_for_id(self, id):
        with self._lock:
            url_set = self.name_bindings[id]
            return list(url_set)
