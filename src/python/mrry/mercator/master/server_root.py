'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy._cperror import HTTPError
import simplejson
import cherrypy
from mrry.mercator.master.datamodel import Session, Worker, WORKER_STATUS_IDLE,\
    Workflow

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
            session.commit()
            id = worker.id
            session.close()
            cherrypy.engine.publish('add_worker', worker)
            return simplejson.dumps(id)
        elif cherrypy.request.method == 'GET':
            session = Session()
            workers = session.query(Worker).all()
            ret = simplejson.dumps([(x.id, x.uri) for x in workers])
            session.close()
            return ret
        raise HTTPError(405)

class WorkflowsRoot:
    
    def __init__(self, scheduler):
        pass
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            session = Session()
            workflow_description = simplejson.loads(cherrypy.request.body.read())
            workflow = Workflow(script_text=workflow_description['text'], script_context=workflow_description['context'])
            session.add(workflow)
            session.commit()
            id = workflow.id
            cherrypy.engine.publish('start_workflow', id)
            session.close()
            return simplejson.dumps(id)
        elif cherrypy.request.method == 'GET':
            session = Session()
            workflows = session.query(Workflow).all()
            ret = simplejson.dumps([w.id for w in workflows])
            session.close()
            return ret
        raise HTTPError(405)

