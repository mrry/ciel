'''
Created on 8 Feb 2010

@author: dgm36
'''
import simplejson
import cherrypy

class PingReceiver:
    
    @cherrypy.expose
    def index(self):
        update = simplejson.loads(cherrypy.request.body.read())
        cherrypy.engine.publish('heartbeat', update)
        
class WorkflowSubmitter:
    
    def __init__(self, status_maintainer):
        self.status_maintainer = status_maintainer
    
    @cherrypy.expose
    def index(self):
        # TODO: handle GET and POST
        pass
    
    @cherrypy.expose
    def default(self, workflow_id):
        # TODO: handle GET
        pass