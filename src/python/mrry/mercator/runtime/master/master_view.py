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
from __future__ import with_statement
from cherrypy import HTTPError
from mrry.mercator.runtime.block_store import json_decode_object_hook,\
    SWReferenceJSONEncoder
from cherrypy.lib.static import serve_file
import sys
import os
import tempfile
import simplejson
import cherrypy
from mrry.mercator.runtime.worker.worker_view import DataRoot

class MasterRoot:
    
    def __init__(self, task_pool, worker_pool, block_store, global_name_directory):
        self.worker = WorkersRoot(worker_pool)
        self.task = MasterTaskRoot(global_name_directory, task_pool)
        self.data = DataRoot(block_store)
        self.global_data = GlobalDataRoot(global_name_directory, task_pool, worker_pool)
        #self.cluster = ClusterDetailsRoot()
        self.shutdown = ShutdownRoot(worker_pool)

    @cherrypy.expose
    def index(self):
        return "Hello from the master!"

class ShutdownRoot:
    
    def __init__(self, worker_pool):
        self.worker_pool = worker_pool
    
    @cherrypy.expose
    def index(self):
        self.worker_pool.shutdown()
        sys.exit(-1)

class WorkersRoot:
    
    def __init__(self, worker_pool):
        self.worker_pool = worker_pool
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            worker_descriptor = simplejson.loads(cherrypy.request.body.read())
            worker_id = self.worker_pool.create_worker(worker_descriptor)
            return simplejson.dumps(worker_id)
        elif cherrypy.request.method == 'GET':
            workers = self.worker_pool.get_all_workers()
            return simplejson.dumps(workers)
        else:
            raise HTTPError(405)
        
    @cherrypy.expose
    def random(self):
        return simplejson.dumps('http://%s/' % (self.worker_pool.get_random_worker().netloc, ))
        
    @cherrypy.expose
    def default(self, worker_id, action=None):
        if cherrypy.request.method == 'POST':
            if action == 'ping':
                ping_contents = simplejson.loads(cherrypy.request.body.read())
                worker_status = ping_contents['status']
                news_list = ping_contents['news']
                cherrypy.engine.publish('worker_ping', int(worker_id), worker_status, news_list)
            elif action == 'stopping':
                cherrypy.engine.publish('worker_failed', int(worker_id))
            else:
                raise HTTPError(404)
        else:
            if action is None:
                return simplejson.dumps(self.worker_pool.get_worker_by_id(int(worker_id)).as_descriptor())

class MasterTaskRoot:
    
    def __init__(self, global_name_directory, task_pool):
        self.global_name_directory = global_name_directory
        self.task_pool = task_pool
        
    # TODO: decide how to submit tasks to the cluster. Effectively, we want to mirror
    #       the way workers do it. Want to have a one-shot distributed execution on the
    #       master before sending out continuations to the cluster. The master should
    #       have minimal exec functionality, and should short-circuit the RPCs for spawning
    #       and creating global IDs.
    #
    #       The master might also have some special functionality that can only be provided
    #       by invoking the master (such as cluster details), but this could potentially be
    #       provided at the master.
       
    @cherrypy.expose 
    def default(self, id=None, action=None):
        if id is not None:
        
            try:
                task_id = int(id)
            except:
                if id == 'flush':
                    self.task_pool.flush_task_dict()
                    return
                else:
                    raise HTTPError(404)
            
            # Spawn and commit actions should probably happen on a buffer object for transactional
            # semantics.
            if action == 'spawn':
                if cherrypy.request.method == 'POST':
                    
                    parent_task = self.task_pool.get_task_by_id(task_id)
#                    if not parent_task.state == TASK_ASSIGNED
                    
                    task_descriptors = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                    spawn_result_ids = []
                    
                    # TODO: stage this in a task-local transaction buffer.
                    for task_d in task_descriptors:
                        try:
                            expected_outputs = task_d['expected_outputs']
                        except KeyError:
                            try:
                                num_outputs = task_d['num_outputs']
                                expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                            except:
                                raise
                            
                            task_d['expected_outputs'] = expected_outputs
                        
                        task = self.task_pool.add_task(task_d, task_id)
                        parent_task.children.append(task.task_id)
                        
                        for global_id in expected_outputs:
                            self.global_name_directory.set_task_for_id(global_id, task.task_id)
                        
                        spawn_result_ids.append(expected_outputs) 
                    
                    return simplejson.dumps(spawn_result_ids)
                    
                else:
                    raise HTTPError(405)
                pass
            elif action == 'commit':
                if cherrypy.request.method == 'POST':
                    commit_payload = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                    commit_bindings = commit_payload['bindings']
                    
                    # Apply commit bindings (if any), i.e. publish results.
                    for global_id, refs in commit_bindings.items():
                        self.global_name_directory.add_refs_for_id(int(global_id), refs)
                    
                    self.task_pool.task_completed(task_id)
                    
                    # Saved continuation URI, if necessary.
                    try:
                        commit_continuation_uri = commit_payload['saved_continuation_uri']
                        self.task_pool.get_task_by_id(task_id).saved_continuation_uri = commit_continuation_uri
                    except KeyError:
                        pass
                    
                    # TODO: Check task for consistency.
                    # TODO: Commit all task activities.

                    return simplejson.dumps(True)
                    
                else:
                    raise HTTPError(405)
            elif action == 'failed':
                if cherrypy.request.method == 'POST':
                    cherrypy.engine.publish('task_failed', task_id, 'RUNTIME_EXCEPTION')
                    return simplejson.dumps(True)
                else:
                    raise HTTPError(405)
            elif action == 'abort':
                self.task_pool.abort(task_id)
            elif action is None:
                if cherrypy.request.method == 'GET':
                    return simplejson.dumps(self.task_pool.tasks[task_id].as_descriptor(long=True), cls=SWReferenceJSONEncoder)
        elif cherrypy.request.method == 'POST':
            # New task spawning in here.
            task_descriptor = simplejson.loads(cherrypy.request.body.read())
            if task_descriptor is not None:
                try:
                    expected_outputs = task_descriptor['expected_outputs']
                except KeyError:
                    try:
                        num_outputs = task_descriptor['num_outputs']
                        expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                    except:
                        expected_outputs = [self.global_name_directory.create_global_id()]
                    task_descriptor['expected_outputs'] = expected_outputs
                
                task = self.task_pool.add_task(task_descriptor)
                return simplejson.dumps({'outputs': expected_outputs, 'task_id': task.task_id})
                        
        else:
            if cherrypy.request.method == 'GET':
                task_fd, filename = tempfile.mkstemp()
                task_file = os.fdopen(task_fd, 'w')
                simplejson.dump(map(lambda x: x.as_descriptor(long=True), self.task_pool.tasks.values()), fp=task_file, cls=SWReferenceJSONEncoder)
                task_file.close()
                return serve_file(filename)
                
class GlobalDataRoot:
    
    def __init__(self, global_name_directory, task_pool, worker_pool):
        self.global_name_directory = global_name_directory
        self.task_pool = task_pool
        self.worker_pool = worker_pool
                
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            # Create a new global ID, and add the POSTed URLs if any.
            refs = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            id = self.global_name_directory.create_global_id(refs)
            return simplejson.dumps(id)
        
    @cherrypy.expose
    def default(self, id, attribute=None):
        if attribute is None:
            if cherrypy.request.method == 'POST':
                # Add a new URL for the global ID.
                real_refs = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                assert real_refs is list
                try:
                    self.global_name_directory.add_refs_for_id(int(id), real_refs)
                    return
                except KeyError:
                    raise HTTPError(404)
            elif cherrypy.request.method == 'GET':
                # Return all URLs for the global ID.
                try:
                    refs = self.global_name_directory.get_refs_for_id(int(id))
                    if len(refs) == 0:
                        cherrypy.response.status = 204
                    return simplejson.dumps(refs, cls=SWReferenceJSONEncoder)
                except KeyError:
                    raise HTTPError(404)
            elif cherrypy.request.method == 'DELETE':
                # Abort task producing the given global ID.
                try:
                    task_id = self.global_name_directory.get_task_for_id(int(id))
                    self.task_pool.abort(task_id)
                except KeyError:
                    raise HTTPError(404)
            raise HTTPError(405)
        elif attribute == 'task':
            if cherrypy.request.method == 'GET':
                task_id = self.global_name_directory.get_task_for_id(int(id))
                task = self.task_pool.get_task_by_id(task_id)
                task_descriptor = task.as_descriptor()
                task_descriptor['is_running'] = task.worker_id is not None
                if task.worker_id is not None:
                    task_descriptor['worker'] = self.worker_pool.get_worker_by_id(task.worker_id).as_descriptor()
                return simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder)
            else:
                raise HTTPError(405)
        elif attribute == 'completion':
            if cherrypy.request.method == 'GET':
                real_id = int(id)
                return simplejson.dumps(self.global_name_directory.wait_for_completion(real_id), cls=SWReferenceJSONEncoder)
            else:
                raise HTTPError(405)
        else:
            raise HTTPError(404)
