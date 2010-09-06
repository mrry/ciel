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
from __future__ import with_statement
from cherrypy import HTTPError
from skywriting.runtime.block_store import json_decode_object_hook,\
    SWReferenceJSONEncoder
from cherrypy.lib.static import serve_file
import sys
import os
import tempfile
import simplejson
import cherrypy
from skywriting.runtime.worker.worker_view import DataRoot

class MasterRoot:
    
    def __init__(self, task_pool, worker_pool, block_store, global_name_directory, job_pool):
        self.worker = WorkersRoot(worker_pool)
        self.job = JobRoot(job_pool)
        self.task = MasterTaskRoot(global_name_directory, task_pool)
        self.streamtask = MasterStreamTaskRoot(global_name_directory, task_pool)
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
            return simplejson.dumps(str(worker_id))
        elif cherrypy.request.method == 'GET':
            workers = [x.as_descriptor() for x in self.worker_pool.get_all_workers()]
            return simplejson.dumps(workers, indent=4)
        else:
            raise HTTPError(405)

    @cherrypy.expose
    def versioned(self):
        if cherrypy.request.method == 'GET':
            (version, workers) = self.worker_pool.get_all_workers_with_version()
            return simplejson.dumps({"version": version, "workers": workers})
        else:
            raise HTTPError(405)

    @cherrypy.expose
    def await_event_count(self, version=0):
        if cherrypy.request.method == 'POST':
            try:
                return simplejson.dumps({ "current_version" : repr(self.worker_pool.await_version_after(int(version))) })
            except Exception as t:
                return simplejson.dumps({ "error": repr(t) })
                
        else:
            raise HTTPError(405)
        
    @cherrypy.expose
    def random(self):
        return simplejson.dumps('http://%s/' % (self.worker_pool.get_random_worker().netloc, ))
        
    @cherrypy.expose
    def default(self, worker_id, action=None):
        try:
            worker = self.worker_pool.get_worker_by_id(worker_id)
        except KeyError:
            raise HTTPError(404)
        if cherrypy.request.method == 'POST':
            if action == 'ping':
                cherrypy.engine.publish('worker_ping', worker)
            elif action == 'stopping':
                cherrypy.engine.publish('worker_failed', worker)
            else:
                raise HTTPError(404)
        else:
            if action is None:
                return simplejson.dumps(worker.as_descriptor())

class JobRoot:
    
    def __init__(self, job_pool):
        self.job_pool = job_pool
        
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            # 1. Read task descriptor from request.
            task_descriptor = simplejson.loads(cherrypy.request.body.read(), 
                                               object_hook=json_decode_object_hook)

            # 2. Add to job pool (synchronously).
            job = self.job_pool.create_job_for_task(task_descriptor)
            
            # 2a. Start job. Possibly do this as deferred work.
            self.job_pool.start_job(job)
            
            # 3. Return descriptor for newly-created job.
            return simplejson.dumps(job.as_descriptor())
            
        elif cherrypy.request.method == 'GET':
            # Return a list of all jobs in the system.
            return simplejson.dumps(self.job_pool.get_all_job_ids())
        else:
            raise HTTPError(405)
        
    @cherrypy.expose
    def default(self, id, attribute=None):
        if cherrypy.request.method != 'GET':
            raise HTTPError(405)
        
        try:
            job = self.job_pool.get_job_by_id(id)
        except KeyError:
            raise HTTPError(404)
        
        if attribute is None:
            # Return the job descriptor as JSON.
            return simplejson.dumps(job.as_descriptor(), cls=SWReferenceJSONEncoder, indent=4)
        elif attribute == 'completion':
            # Block until the job is completed.
            self.job_pool.wait_for_completion(job)
            return simplejson.dumps(job.as_descriptor(), cls=SWReferenceJSONEncoder) 
        else:
            # Invalid attribute.
            raise HTTPError(404)
        

class MasterStreamTaskRoot:
    
    def __init__(self, global_name_directory, task_pool):
        self.global_name_directory = global_name_directory
        self.task_pool = task_pool

    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'GET':

            def index_gen():
                enc = SWReferenceJSONEncoder()
                evtcount = self.task_pool.event_index
                vals = self.task_pool.tasks.values()
                if len(vals) == 0:
                    yield "{\"taskmap\": [], \"evtcount\": " + str(evtcount) + "}"
                else:
                    yield ("{\"taskmap\": [" + enc.encode(vals[0].as_descriptor(long=True)))
                    for x in vals[1:]:
                        next_desc = x.as_descriptor(long=True)
                        yield ("," + enc.encode(next_desc))
                    yield "], \"evtcount\": " + str(evtcount) + "}"
                           
            # Produces a JSON generator which itself feeds off a descriptor-generator
            # return enc.iterencode([x.as_descriptor(long=True) for x in self.task_pool.tasks.values()])
            # (Too slow)
            return index_gen()

        else:
            raise HTTPError(405)
    index._cp_config = {'response.stream': True}


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
        
            task_id = id
            if id == 'flush':
                self.task_pool.flush_task_dict()
                return
            elif id == "wait_event_count":
                try:
                    self.task_pool.wait_event_after(int(action))
                    return simplejson.dumps({ "latest_event_index" : self.task_pool.event_index })
                except Exception as t:
                    return simplejson.dumps({ "error" : repr(t)})

            elif id == "events":
                report_event_index = self.task_pool.event_index
                start_event_index = int(action)
                finish_event_index = None
                if ((report_event_index - start_event_index) > 1000):
                    # Maximum report size; should make client-configurable
                    finish_event_index = start_event_index + 1000
                else:
                    finish_event_index = report_event_index
                events_to_send = self.task_pool.events[start_event_index:finish_event_index]
                #sys.stderr.write("Events: %s. Sending %d-%d == %s\n" % (repr(self.task_pool.events), start_event_index, finish_event_index, repr(events_to_send)))
                events_reply = {"events": events_to_send, 
                                "next_event_to_fetch": finish_event_index, 
                                "first_unavailable_event": report_event_index}
                return simplejson.dumps(events_reply, cls=SWReferenceJSONEncoder)
            
            if action == 'spawn':
                if cherrypy.request.method == 'POST':
                    parent_task = self.task_pool.get_task_by_id(task_id)
                    task_descriptors = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                    self.task_pool.spawn_child_tasks(parent_task, task_descriptors)
                    return
                else:
                    raise HTTPError(405)
            elif action == 'commit':
                if cherrypy.request.method == 'POST':
                    commit_payload = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                    self.task_pool.commit_task(task_id, commit_payload)
                    return simplejson.dumps(True)
                else:
                    raise HTTPError(405)
            elif action == 'failed':
                if cherrypy.request.method == 'POST':
                    task = self.task_pool.get_task_by_id(task_id)
                    failure_payload = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                    cherrypy.engine.publish('task_failed', task, failure_payload)
                    return simplejson.dumps(True)
                else:
                    raise HTTPError(405)
            elif action == 'abort':
                self.task_pool.abort(task_id)
            elif action is None:
                if cherrypy.request.method == 'GET':
                    return simplejson.dumps(self.task_pool.get_task_by_id(task_id).as_descriptor(long=True), cls=SWReferenceJSONEncoder)
        elif cherrypy.request.method == 'POST':
            # New task spawning in here.
            task_descriptor = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            spawned_task_id = self.task_pool.generate_task_id()
            task_descriptor['task_id'] = str(spawned_task_id)
            try:
                expected_outputs = task_descriptor['expected_outputs']
                for output in expected_outputs:
                    self.global_name_directory.create_global_id(spawned_task_id, output)
            except KeyError:
                try:
                    num_outputs = task_descriptor['num_outputs']
                    expected_outputs = map(lambda x: self.global_name_directory.create_global_id(spawned_task_id), range(0, num_outputs))
                except:
                    expected_outputs = [self.global_name_directory.create_global_id()]
                task_descriptor['expected_outputs'] = expected_outputs
            task = self.task_pool.add_task(task_descriptor)
            return simplejson.dumps({'outputs': map(str, expected_outputs), 'task_id': str(task.task_id)})
                        
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
            return simplejson.dumps(str(id))
        
    @cherrypy.expose
    def default(self, id, attribute=None):
        
        real_id = id
        
        if attribute is None:
            if cherrypy.request.method == 'POST':
                # Add a new URL for the global ID.
                real_refs = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
                assert real_refs is list
                try:
                    self.global_name_directory.add_refs_for_id(real_id, real_refs)
                    return
                except KeyError:
                    raise HTTPError(404)
            elif cherrypy.request.method == 'GET':
                # Return all URLs for the global ID.
                try:
                    refs = self.global_name_directory.get_refs_for_id(real_id)
                    if len(refs) == 0:
                        cherrypy.response.status = 204
                    return simplejson.dumps(refs, cls=SWReferenceJSONEncoder)
                except KeyError:
                    raise HTTPError(404)
            elif cherrypy.request.method == 'DELETE':
                # Abort task producing the given global ID.
                try:
                    task_id = self.global_name_directory.get_task_for_id(real_id)
                    self.task_pool.abort(task_id)
                except KeyError:
                    raise HTTPError(404)
            raise HTTPError(405)
        elif attribute == 'task':
            if cherrypy.request.method == 'GET':
                task_id = self.global_name_directory.get_task_for_id(real_id)
                task = self.task_pool.get_task_by_id(task_id)
                task_descriptor = task.as_descriptor(long=True)
                task_descriptor['is_running'] = task.worker is not None
                if task.worker is not None:
                    task_descriptor['worker'] = task.worker.as_descriptor()
                return simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder)
            else:
                raise HTTPError(405)
        elif attribute == 'completion':
            if cherrypy.request.method == 'GET':
                try:
                    ret = {"refs" : self.global_name_directory.wait_for_completion(real_id)}
                    return simplejson.dumps(ret, cls=SWReferenceJSONEncoder)
                except Exception as t:
                    return simplejson.dumps({"error" : repr(t)})

            else:
                raise HTTPError(405)
        else:
            raise HTTPError(404)
