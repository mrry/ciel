'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy._cperror import HTTPError
from mrry.mercator.master.workflow import build_workflow
from cherrypy.lib.static import serve_file
import simplejson
import cherrypy
from mrry.mercator.master.datamodel import Session, Worker, WORKER_STATUS_IDLE,\
    engine
import time

class MasterRoot:
    
    def __init__(self):
        self.ping = PingReceiver()
        self.worker = WorkersRoot()
        self.workflow = WorkflowsRoot(None)

class PingReceiver:
    
    @cherrypy.expose
    def index(self):
        update_tuple = simplejson.loads(cherrypy.request.body.read())
        worker_id = update_tuple[0]
        update_list = update_tuple[1]
        
        for update in update_list:
            cherrypy.engine.publish("ping_received", worker_id, update)

class WorkersRoot:
    
    def __init__(self):
        pass
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            session = Session()
            worker_description = simplejson.loads(cherrypy.request.body.read())
            worker = Worker(uri=worker_description['uri'], status=WORKER_STATUS_IDLE)
            session.add(worker)
            id = worker.id
            session.commit()
            session.close()
            cherrypy.engine.publish('add_worker', worker)
            return simplejson.dumps(id)
        raise HTTPError(405)

class WorkflowsRoot:
    
    def __init__(self, scheduler):
        pass
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            workflow_description = simplejson.loads(cherrypy.request.body.read())
            workflow = build_workflow(workflow_description)
            cherrypy.engine.publish('create_workflow', workflow)
            return simplejson.dumps(workflow.id)
        raise HTTPError(405)
    
    @cherrypy.expose
    def default(self, workflow_id, job_id):
        if cherrypy.request.method == 'POST':
            job_result = simplejson.loads(cherrypy.request.body.read())
            cherrypy.engine.publish('job_completed', workflow_id, job_id, job_result)
            return
        raise HTTPError(405)
