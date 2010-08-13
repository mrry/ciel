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
Created on Apr 15, 2010

@author: derek
'''
from __future__ import with_statement
from threading import Lock, Condition
import cherrypy
import uuid

class GlobalNameDirectoryEntry:
    def __init__(self, task_id, refs):
        self.refs = refs
        self.task_id = task_id
        self.waiters = []
        
class GlobalNameDirectory:
    
    def __init__(self):
        self._lock = Lock()
        self.directory = {}
    
    def create_global_id(self, task_id=None, data_id=None):
        try:
            entry = self.directory[data_id]
            entry.task_id = task_id
        except:
            entry = GlobalNameDirectoryEntry(task_id, [])
            if data_id is None:
                data_id = uuid.uuid1()
            self.directory[data_id] = entry
        return data_id
    
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
