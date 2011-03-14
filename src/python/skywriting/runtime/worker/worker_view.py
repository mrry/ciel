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
from skywriting.runtime.executors import kill_all_running_children
from shared.references import SW2_FetchReference, SW2_ConcreteReference
import logging
import StringIO

'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.lib.static import serve_file
from skywriting.runtime.block_store import json_decode_object_hook,\
    SWReferenceJSONEncoder
import sys
import simplejson
import cherrypy
import os
import time
import ciel
import logging

class WorkerRoot:
    
    def __init__(self, worker):
        self.worker = worker
        self.control = ControlRoot(worker)
        self.data = self.control.data

class ControlRoot:

    def __init__(self, worker):
        self.worker = worker
        self.master = RegisterMasterRoot(worker)
        self.task = TaskRoot(worker)
        self.data = DataRoot(worker.block_store)
        self.streamstat = StreamStatRoot(worker.block_store)
        self.features = FeaturesRoot(worker.execution_features)
        self.kill = KillRoot()
        self.log = LogRoot(worker)
        self.upload = UploadRoot(worker.upload_manager)
        self.admin = ManageRoot(worker.block_store)
        self.fetch = FetchRoot(worker.upload_manager)
        self.process = ProcessRoot(worker.process_pool)
        self.abort = AbortRoot(worker)
    
    @cherrypy.expose
    def index(self):
        return simplejson.dumps(self.worker.id)

class StreamStatRoot:

    def __init__(self, block_store):
        self.block_store = block_store

    @cherrypy.expose
    def default(self, id, op):
        if cherrypy.request.method == "POST":
            payload = simplejson.loads(cherrypy.request.body.read())
            if op == "subscribe":
                self.block_store.subscribe_to_stream(payload["netloc"], payload["chunk_size"], id)
            elif op == "unsubscribe":
                self.block_store.unsubscribe_from_stream(payload["netloc"], id)
            elif op == "advert":
                self.block_store.receive_stream_advertisment(id, **payload)
            else:
                raise cherrypy.HTTPError(404)
        else:
            raise cherrypy.HTTPError(405)

class KillRoot:
    
    def __init__(self):
        pass
    
    @cherrypy.expose
    def index(self):
        kill_all_running_children()
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
                self.worker.multiworker.create_and_queue_taskset(task_descriptor)
                return
        raise cherrypy.HTTPError(405)
    
    # TODO: Add some way of checking up on the status of a running task.
    #       This should grow to include a way of getting the present activity of the task
    #       and a way of setting breakpoints.
    #       ...and a way of killing the task.
    #       Ideally, we should create a task view (Root) for each running task.    

class AbortRoot:
    
    def __init__(self, worker):
        self.worker = worker
    
    @cherrypy.expose
    def default(self, job_id, task_id=None):
        
        try:
            job = self.worker.multiworker.get_job_by_id(job_id)
        except KeyError:
            return
            
        if task_id is None:
            job.abort_all_active_tasksets()
        else:
            job.abort_taskset_with_id(task_id)
            

class LogRoot:

    def __init__(self, worker):
        self.worker = worker

    @cherrypy.expose
    def wait_after(self, event_count):
        if cherrypy.request.method == 'POST':
            try:
                self.worker.await_log_entries_after(event_count)
                return simplejson.dumps({})
            except Exception as e:
                return simplejson.dumps({"error": repr(e)})
        else:
            raise cherrypy.HTTPError(405)

    @cherrypy.expose
    def index(self, first_event, last_event):
        if cherrypy.request.method == 'GET':
            events = self.worker.get_log_entries(int(first_event), int(last_event))
            return simplejson.dumps([{"time": repr(t), "event": e} for (t, e) in events])
        else:
            raise cherrypy.HTTPError(405)

class DataRoot:
    
    def __init__(self, block_store, backup_sender=None):
        self.block_store = block_store
        self.backup_sender = backup_sender
        
    @cherrypy.expose
    def default(self, id):
        safe_id = id
        if cherrypy.request.method == 'GET':
            filename = self.block_store.filename(safe_id)
            try:
                response_body = serve_file(filename)
                return response_body
            except cherrypy.HTTPError as he:
                if he.status == 404:
                    response_body = serve_file(self.block_store.producer_filename(safe_id))
                    return response_body
                else:
                    raise
                
        elif cherrypy.request.method == 'POST':
            request_body = cherrypy.request.body.read()
            new_ref = self.block_store.ref_from_string(request_body, safe_id)
            if self.backup_sender is not None:
                self.backup_sender.add_data(safe_id, request_body)
            #if self.task_pool is not None:
            #    self.task_pool.publish_refs({safe_id : new_ref})
            return simplejson.dumps(new_ref, cls=SWReferenceJSONEncoder)
        
        elif cherrypy.request.method == 'HEAD':
            if os.path.exists(self.block_store.filename(id)):
                return
            else:
                raise cherrypy.HTTPError(404)

        else:
            raise cherrypy.HTTPError(405)

    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            id = self.block_store.allocate_new_id()
            request_body = cherrypy.request.body.read()
            new_ref = self.block_store.ref_from_string(request_body, id)
            if self.backup_sender is not None:
                self.backup_sender.add_data(id, request_body)
            #if self.task_pool is not None:
            #    self.task_pool.publish_refs({id : new_ref})
            return simplejson.dumps(new_ref, cls=SWReferenceJSONEncoder)
        elif cherrypy.request.method == 'GET':
            return serve_file(self.block_store.generate_block_list_file())
        else:
            raise cherrypy.HTTPError(405)
        
    # TODO: Also might investigate a way for us to have a spanning tree broadcast
    #       for common files.
    
class UploadRoot:
    
    def __init__(self, upload_manager):
        self.upload_manager = upload_manager
        
    @cherrypy.expose
    def default(self, id, start=None):
        if cherrypy.request.method != 'POST':
            raise cherrypy.HTTPError(405)
        if start is None:
            #upload_descriptor = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            self.upload_manager.start_upload(id)#, upload_descriptor)
        elif start == 'commit':
            size = int(simplejson.load(cherrypy.request.body))
            self.upload_manager.commit_upload(id, size)
        else:
            start_index = int(start)
            self.upload_manager.handle_chunk(id, start_index, cherrypy.request.body)
    
class FetchRoot:
    
    def __init__(self, upload_manager):
        self.upload_manager = upload_manager
        
    @cherrypy.expose
    def default(self, id=None):
        if cherrypy.request.method != 'POST':
            if id is None:
                return simplejson.dumps(self.upload_manager.current_fetches)
            else:
                status = self.upload_manager.get_status_for_fetch(id)
                if status != 200:
                    raise cherrypy.HTTPError(status)
                else:
                    return
                
        refs = simplejson.load(cherrypy.request.body, object_hook=json_decode_object_hook)
        self.upload_manager.fetch_refs(id, refs)
        
        cherrypy.response.status = '202 Accepted'
        
class ProcessRoot:
    
    def __init__(self, process_pool):
        self.process_pool = process_pool
        
    @cherrypy.expose
    def default(self, id=None):
        
        if cherrypy.request.method == 'POST' and id is None:
            # Create a new process record.
            (pid, protocol) = simplejson.load(cherrypy.request.body)
            record = self.process_pool.create_process_record(pid, protocol)
            self.process_pool.create_job_for_process(record)
            
            return simplejson.dumps(record.as_descriptor())
            
        elif cherrypy.request.method == 'GET' and id is None:
            # Return a list of process IDs.
            return simplejson.dumps(self.process_pool.get_process_ids())
        
        elif id is not None:
            # Return information about a running process (for debugging).
            record = self.process_pool.get_process_record(id)
            if record is None:
                raise cherrypy.HTTPError(404)
            elif cherrypy.request.method != 'GET':
                raise cherrypy.HTTPError(405)
            else:
                return simplejson.dumps(record.as_descriptor())
        
        else:
            raise cherrypy.HTTPError(405)
        
class ManageRoot:
    
    def __init__(self, block_store):
        self.block_store = block_store
        
    @cherrypy.expose
    def default(self, action=None, id=None):
        if action == 'flush':
            if id == 'really':
                kept, removed = self.block_store.flush_unpinned_blocks(True)
                return 'Kept %d blocks, removed %d blocks' % (kept, removed)
            else:
                kept, removed = self.block_store.flush_unpinned_blocks(False)
                return 'Would keep %d blocks, remove %d blocks' % (kept, removed)
        elif action == 'pin' and id is not None:
            self.block_store.pin_ref_id(id)
        elif action == 'pin':
            return simplejson.dumps(self.block_store.generate_pin_refs(), cls=SWReferenceJSONEncoder)
    
class FeaturesRoot:
    
    def __init__(self, execution_features):
        self.execution_features = execution_features
    
    @cherrypy.expose
    def index(self):
        return simplejson.dumps(self.execution_features.all_features())
