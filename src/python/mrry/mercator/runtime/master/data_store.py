'''
Created on Apr 15, 2010

@author: derek
'''
from threading import Lock
import cherrypy

class GlobalNameDirectory:
    
    def __init__(self):
        self._lock = Lock()
        self.current_id = 0
        self.name_bindings = {}
    
    def create_global_id(self, urls=[]):
        url_set = set()
        with self._lock:
            id = self.current_id
            self.current_id += 1
            self.name_bindings[id] = url_set
        for url in urls:
            url_set.add(url)
        return id
    
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