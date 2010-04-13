'''
Created on 8 Feb 2010

@author: dgm36
'''
from mrry.mercator.jobmanager.job import build_job
from cherrypy.lib.static import serve_file
import simplejson
import cherrypy

class ServerRoot:
    
    def __init__(self, status_maintainer):
        self.status_maintainer = status_maintainer
    
    @cherrypy.expose
    def index(self):
        return "Hello from the job manager server...."
    
class JobsRoot:
    
    def __init__(self, status_maintainer, job_runner, data_manager):
        self.status_maintainer = status_maintainer
        self.job_runner = job_runner
        self.data = DataRoot(data_manager)
    
    @cherrypy.expose    
    def index(self):
        if cherrypy.request.method == 'POST':
            job = build_job(simplejson.loads(cherrypy.request.body.read()))
            if job is not None:
                cherrypy.engine.publish('create_job', job)
                return simplejson.dumps(str(job.id))
            else:
                # Job type not supported.
                raise cherrypy.HTTPError(500)
        elif cherrypy.request.method == 'GET':
            return simplejson.dumps(self.status_maintainer.list_jobs())
        
    @cherrypy.expose
    def default(self, job_id):
        if cherrypy.request.method == 'POST':
            self.job_runner.send_to_process(job_id, cherrypy.request.body.read())
            return None
        elif cherrypy.request.method == 'GET':
            status = self.status_maintainer.get_status(job_id)
            if status is None:
                raise cherrypy.HTTPError(404)
            return simplejson.dumps(status)
        
class DataRoot:
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        
    @cherrypy.expose
    def index(self):
        return simplejson.dumps(self.data_manager.get_all_data())
        
    @cherrypy.expose
    def default(self, datum_id):
        try:
            datum_filename = self.data_manager.get_filename(datum_id)
        except:
            raise cherrypy.HTTPError(404)
        return serve_file(datum_filename)