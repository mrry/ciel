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
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.lib.static import serve_file
from skywriting.runtime.block_store import json_decode_object_hook
import sys
import simplejson
import cherrypy
import uuid

class WorkerRoot:
    
    def __init__(self, worker):
        self.master = RegisterMasterRoot(worker)
        self.task = TaskRoot(worker)
        self.data = DataRoot(worker.block_store)
        self.features = FeaturesRoot(worker.execution_features)
        self.kill = KillRoot()
    
    @cherrypy.expose
    def index(self):
        return "Hello from the worker...."

class KillRoot:
    
    def __init__(self):
        pass
    
    @cherrypy.expose
    def index(self):
        sys.exit(0)

class RegisterMasterRoot:
    
    def __init__(self, worker):
        self.worker = worker
            
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            master_details = simplejson.loads(cherrypy.request.body.read())
            self.worker.set_master(master_details)
        elif cherrypy.request.method == 'GET':
            return simplejson.dumps(self.worker.master_proxy.get_master_details())
        else:
            raise cherrypy.HTTPError(405)
    
class TaskRoot:
    
    def __init__(self, worker):
        self.worker = worker
        
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            task_descriptor = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            if task_descriptor is not None:
                self.worker.submit_task(task_descriptor)
                return
        raise cherrypy.HTTPError(405)
    
    @cherrypy.expose
    def default(self, task_id, action):
        real_id = uuid.UUID(hex=task_id)
        if action == 'abort':
            if cherrypy.request.method == 'POST':
                self.worker.abort_task(task_id)
            else:
                raise cherrypy.HTTPError(405)
        else:
            raise cherrypy.HTTPError(404)
    
    # TODO: Add some way of checking up on the status of a running task.
    #       This should grow to include a way of getting the present activity of the task
    #       and a way of setting breakpoints.
    #       ...and a way of killing the task.
    #       Ideally, we should create a task view (Root) for each running task.    


class DataRoot:
    
    def __init__(self, block_store):
        self.block_store = block_store
        
    @cherrypy.expose
    def default(self, id):
        safe_id = uuid.UUID(hex=id)
        if cherrypy.request.method == 'GET':
            return serve_file(self.block_store.filename(safe_id))
        elif cherrypy.request.method == 'POST':
            url = self.block_store.store_raw_file(cherrypy.request.body, safe_id)
            return simplejson.dumps(url)
        else:
            raise cherrypy.HTTPError(405)

    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            url = self.block_store.store_raw_file(cherrypy.request.body, self.block_store.allocate_new_id())
            return simplejson.dumps(url)
        elif cherrypy.request.method == 'GET':
            return serve_file(self.block_store.generate_block_list_file())
        else:
            raise cherrypy.HTTPError(405)
        
    # TODO: Also might investigate a way for us to have a spanning tree broadcast
    #       for common files.
    
class FeaturesRoot:
    
    def __init__(self, execution_features):
        self.execution_features = execution_features
    
    @cherrypy.expose
    def index(self):
        return simplejson.dumps(self.execution_features.all_features())