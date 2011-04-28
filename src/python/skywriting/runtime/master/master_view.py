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
from shared.references import json_decode_object_hook,\
    SWReferenceJSONEncoder
import sys
import simplejson
import cherrypy
from skywriting.runtime.worker.worker_view import DataRoot
from skywriting.runtime.master.cluster_view import WebBrowserRoot
import ciel
import logging
import socket
from skywriting.runtime.task_graph import TaskGraphUpdate

class MasterRoot:
    
    def __init__(self, worker_pool, block_store, job_pool, backup_sender, monitor):
        self.control = ControlRoot(worker_pool, block_store, job_pool, backup_sender, monitor)
        self.data = self.control.data

class ControlRoot:

    def __init__(self, worker_pool, block_store, job_pool, backup_sender, monitor):
        self.worker = WorkersRoot(worker_pool, backup_sender, monitor)
        self.job = JobRoot(job_pool, backup_sender, monitor)
        self.task = MasterTaskRoot(job_pool, worker_pool, backup_sender)
        self.data = DataRoot(block_store, backup_sender)
        self.gethostname = HostnameRoot()
        self.shutdown = ShutdownRoot(worker_pool)
        self.browse = WebBrowserRoot(job_pool)
        self.backup = BackupMasterRoot(backup_sender)
        self.ref = RefRoot(job_pool)

    @cherrypy.expose
    def index(self):
        return "Hello from the master!"    

class HostnameRoot:

    @cherrypy.expose
    def index(self):
        (name, _, _) = socket.gethostbyaddr(cherrypy.request.remote.ip)
        return simplejson.dumps(name)

class ShutdownRoot:
    
    def __init__(self, worker_pool):
        self.worker_pool = worker_pool
    
    @cherrypy.expose
    def index(self):
        self.worker_pool.shutdown()
        sys.exit(-1)

class WorkersRoot:
    
    def __init__(self, worker_pool, backup_sender, monitor=None):
        self.worker_pool = worker_pool
        self.backup_sender = backup_sender
        self.monitor = monitor
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            request_body = cherrypy.request.body.read()
            worker_descriptor = simplejson.loads(request_body)
            if self.monitor is not None and not self.monitor.is_primary:
                self.monitor.add_worker(worker_descriptor['netloc'])
                self.backup_sender.add_worker(request_body)
            else:
                worker_id = self.worker_pool.create_worker(worker_descriptor)
                self.backup_sender.add_worker(request_body)
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
            if worker_id == 'reset':
                self.worker_pool.reset()
                return
            else:
                raise HTTPError(404)
        if cherrypy.request.method == 'POST':
            if action == 'ping':
                self.worker_pool.worker_ping(worker)
            elif action == 'stopping':
                self.worker_pool.worker_failed(worker)
            else:
                raise HTTPError(404)
        else:
            if action is None:
                return simplejson.dumps(worker.as_descriptor())

class JobRoot:
    
    def __init__(self, job_pool, backup_sender, monitor=None):
        self.job_pool = job_pool
        self.backup_sender = backup_sender
        self.monitor = monitor
        
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            # 1. Read task descriptor from request.
            request_body = cherrypy.request.body.read()
            payload = simplejson.loads(request_body, 
                                               object_hook=json_decode_object_hook)

            task_descriptor = payload['root_task']
            try:
                job_options = payload['job_options']
            except KeyError:
                job_options = {}

            # 2. Add to job pool (synchronously).
            job = self.job_pool.create_job_for_task(task_descriptor, job_options)
            
            # 2bis. Send to backup master.
            self.backup_sender.add_job(job.id, request_body)
            
            # 2a. Start job. Possibly do this as deferred work.
            self.job_pool.queue_job(job)
                        
            # 3. Return descriptor for newly-created job.
            return simplejson.dumps(job.as_descriptor())
            
        elif cherrypy.request.method == 'GET':
            # Return a list of all jobs in the system.
            return simplejson.dumps(self.job_pool.get_all_job_ids())
        else:
            raise HTTPError(405)

    @cherrypy.expose
    def default(self, id, attribute=None):
        if cherrypy.request.method == 'POST':
            ciel.log('Creating job for ID: %s' % id, 'JOB_POOL', logging.INFO)
            # Need to support this for backup masters, so that the job ID is
            # consistent.
            
            # 1. Read task descriptor from request.
            request_body = cherrypy.request.body.read()
            task_descriptor = simplejson.loads(request_body, 
                                               object_hook=json_decode_object_hook)

            # 2. Add to job pool (synchronously).
            job = self.job_pool.create_job_for_task(task_descriptor, job_id=id)

            # 2bis. Send to backup master.
            self.backup_sender.add_job(job.id, request_body)
            
            if self.monitor is not None and self.monitor.is_primary:
                # 2a. Start job. Possibly do this as deferred work.
                self.job_pool.queue_job(job)
            else:
                ciel.log('Registering job, but not starting it: %s' % job.id, 'JOB_POOL', logging.INFO)
                #self.job_pool.task_pool.add_task(job.root_task, False)
            
            # 3. Return descriptor for newly-created job.
            
            ciel.log('Done handling job POST', 'JOB_POOL', logging.INFO)
            return simplejson.dumps(job.as_descriptor())            
        
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
        
class MasterTaskRoot:
    
    def __init__(self, job_pool, worker_pool, backup_sender):
        self.job_pool = job_pool
        self.worker_pool = worker_pool
        self.backup_sender = backup_sender
       
    @cherrypy.expose 
    def default(self, job_id, task_id, action=None):
        
        try:
            job = self.job_pool.get_job_by_id(job_id)
        except KeyError:
            ciel.log('No such job: %s' % job_id, 'MASTER', logging.ERROR)
            raise HTTPError(404)

        try:
            task = job.task_graph.get_task(task_id)
        except KeyError:
            ciel.log('No such task: %s in job: %s' % (task_id, job_id), 'MASTER', logging.ERROR)
            raise HTTPError(404)

        if cherrypy.request.method == 'GET':
            if action is None:
                return simplejson.dumps(task.as_descriptor(long=True), cls=SWReferenceJSONEncoder)
            else:
                ciel.log('Invalid operation: cannot GET with an action', 'MASTER', logging.ERROR)
                raise HTTPError(405)
        elif cherrypy.request.method != 'POST':
            ciel.log('Invalid operation: only POST is supported for task operations', 'MASTER', logging.ERROR)
            raise HTTPError(405)

        # Action-handling starts here.

        if action == 'report':
            # Multi-spawn-and-commit
            report_payload = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            worker = self.worker_pool.get_worker_by_id(report_payload['worker'])
            report = report_payload['report']
            job.report_tasks(report, task, worker)
            return

        elif action == 'failed':
            failure_payload = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            job.investigate_task_failure(task, failure_payload)
            return simplejson.dumps(True)
        
        elif action == 'publish':
            request_body = cherrypy.request.body.read()
            refs = simplejson.loads(request_body, object_hook=json_decode_object_hook)
            
            tx = TaskGraphUpdate()
            for ref in refs:
                tx.publish(ref, task)
            tx.commit(job.task_graph)
            job.schedule()

            self.backup_sender.publish_refs(task_id, refs)
            return
            
        elif action == 'log':
            # Message body is a JSON list containing UNIX timestamp in seconds and a message string.
            request_body = cherrypy.request.body.read()
            timestamp, message = simplejson.loads(request_body, object_hook=json_decode_object_hook)
            ciel.log("%s %f %s" % (task_id, timestamp, message), 'TASK_LOG', logging.INFO)
            
        elif action == 'abort':
            # FIXME (maybe): There is currently no abort method on Task.
            task.abort(task_id)
            return
        
        elif action is None:
            ciel.log('Invalid operation: only GET is supported for tasks', 'MASTER', logging.ERROR)
            raise HTTPError(404)
        else:
            ciel.log('Unknown action (%s) on task (%s)' % (action, task_id), 'MASTER', logging.ERROR)
            raise HTTPError(404)
            
            
class BackupMasterRoot:
    
    def __init__(self, backup_sender):
        self.backup_sender = backup_sender
        
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            # Register the  a new global ID, and add the POSTed URLs if any.
            standby_url = simplejson.loads(cherrypy.request.body.read(), object_hook=json_decode_object_hook)
            self.backup_sender.register_standby_url(standby_url)
            return 'Registered a hot standby'
        
class RefRoot:
    
    def __init__(self, job_pool):
        self.job_pool = job_pool

    @cherrypy.expose         
    def default(self, job_id, ref_id):
        
        try:
            job = self.job_pool.get_job_by_id(job_id)
        except KeyError:
            raise HTTPError(404)
        
        try:
            ref = job.task_graph.get_reference_info(ref_id).ref
        except KeyError:
            raise HTTPError(404)

        return simplejson.dumps(ref, cls=SWReferenceJSONEncoder)
