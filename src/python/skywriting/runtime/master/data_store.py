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
from cherrypy.process import plugins
import uuid

class GlobalNameDirectoryEntry:
    def __init__(self, task_id, refs, lock):
        self.refs = refs
        self.task_id = task_id
        self.cond = Condition(lock)
        
class GlobalNameDirectory(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self._lock = Lock()
        self.directory = {}
        self.bus = bus
        self.is_stopping = False
        self.current_waiters = 0
        self.max_concurrent_waiters = 10
    
    def subscribe(self):
        self.bus.subscribe("stop", self.server_stopping, 10)
        # Higher priority than the HTTP server
    
    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.server_stopping)

    def create_global_id(self, task_id=None, data_id=None):
        try:
            entry = self.directory[data_id]
            entry.task_id = task_id
        except:
            entry = GlobalNameDirectoryEntry(task_id, [], self._lock)
            if data_id is None:
                data_id = str(uuid.uuid1())
            self.directory[data_id] = entry
        return data_id
    
    def get_task_for_id(self, id):
        return self.directory[id].task_id
    
    def set_task_for_id(self, id, task_id):
        self.directory[id].task_id = task_id
    
    def delete_all_refs_for_id(self, id):
        try:
            with self._lock:
                entry = self.directory[id]
                entry.refs = []
        except KeyError:
            self.create_global_id(None, id)
    
    def add_refs_for_id(self, id, refs):
        with self._lock:
            entry = self.directory[id]
            for ref in refs:
                entry.refs.append(ref)
            entry.cond.notify_all()
        cherrypy.engine.publish('global_name_available', id, entry.refs)
    
    def get_refs_for_id(self, id):
        return self.directory[id].refs

    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            for entry in self.directory.values():
                entry.cond.notify_all()

    def wait_for_completion(self, id):
        with self._lock:
            entry = self.directory[id]
            while (not self.is_stopping) and (len(entry.refs) == 0):
                if self.is_stopping:
                    break
                elif self.current_waiters > self.max_concurrent_waiters:
                    break
                else:
                    entry.cond.wait()
            if self.is_stopping:
                raise Exception("Server stopping")
            elif self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")
            else:
                return entry.refs
