'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.lib.static import serve_file
import simplejson
import cherrypy

class WorkerRoot:
    
    def __init__(self, status_maintainer, block_store, exec_facility):
        self.status_maintainer = status_maintainer
        self.task = TaskRoot()
        self.data = DataRoot(block_store)
        self.features = FeaturesRoot(exec_facility)
    
    @cherrypy.expose
    def index(self):
        return "Hello from the job manager server...."
    
class TaskRoot:
    
    def __init__(self):
        pass
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            task_descriptor = simplejson.loads(cherrypy.request.body.read())
            if task_descriptor is not None:
                cherrypy.engine.publish('execute_task', task_descriptor)
                return
        raise cherrypy.HTTPError(405)
    
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
        safe_id = int(id)
        return serve_file(self.block_store.filename(safe_id))

    # TODO: have a way from outside the cluster to push some data to a node.
    #       Also might investigate a way for us to have a spanning tree broadcast
    #       for common files.
    
class FeaturesRoot:
    
    def __init__(self, exec_facility):
        self.exec_facility = exec_facility
        
    def index(self):
        return simplejson.dumps(self.exec_facility.all_facilities())