'''
Created on 8 Feb 2010

@author: dgm36
'''
import simplejson
import cherrypy

class MasterRoot:
    
    def __init__(self, scheduler):
        self.ping = PingReceiver()
        self.workflow = WorkflowSubmitter(scheduler)

class PingReceiver:
    
    @cherrypy.expose
    def index(self):
        update = simplejson.loads(cherrypy.request.body.read())
        cherrypy.engine.publish('ping', update)
        
class WorkflowSubmitter:
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
    
    @cherrypy.expose
    def index(self):
        # TODO: handle GET and POST
        pass
    
    @cherrypy.expose
    def default(self, workflow_id):
        # TODO: handle GET
        pass