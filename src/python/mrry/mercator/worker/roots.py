'''
Created on 1 Feb 2010

@author: dgm36
'''
from mrry.mercator.worker.messages import Message
import simplejson
import cherrypy

class DefaultJobRoot:
    
    def __init__(self, jobName):
        self.jobName = jobName
    
    @cherrypy.expose
    def index(self):
        return "Default root for job %s" % (self.jobName, )
    
class DefaultActiveJobRoot:
    
    def __init__(self, job):
        self.job = job
        
    def index(self):
        if cherrypy.request.method == 'POST':
            request_dict = simplejson.loads(cherrypy.request.body)
            request_msg = QueryMessage(request_dict)
            
            response_msg = do_synchronous_request(request_msg, job.requestQueue)
            
            return simplejson.dumps(response_msg.fieldDict)
            
        else:
            return "Why are you GETting me?"