'''
Created on 28 Jan 2010

@author: dgm36
'''
import time
import datetime
import uuid
import cherrypy
import simplejson

   
class JobFacilityRoot:

    def __init__(self, facility):
        self.facility = facility

    @cherrypy.expose
    def index(self, x):
        if cherrypy.request.method == "POST":
            job = self.facility.createJob()
            return job.name
        else:
            return "JobFacility: %s" % (self.facility.name, )

class FacilityRoot:

    def __init__(self, facility):
        self.facility = facility

    @cherrypy.expose
    def index(self):
        return "Facility: %s" % (self.facility.name, )

class JobsRoot:
    
    def __init__(self, worker):
        self.worker = worker
    
    @cherrypy.expose
    def index(self):
        jobstring = "\n".join([job.name for job in self.worker.jobs.values()])
        return jobstring
        
class WorkerRoot:
    
    def __init__(self, worker):
        self.worker = worker
        for (name, facility) in worker.facilities.items():
            setattr(self, name, facility.root())
            
        self.jobs = JobsRoot(self.worker)

    @cherrypy.expose
    def index(self):
        return "Worker: %s" % (self.worker.name, )

class Worker:
    
    def __init__(self, name, task_queue):
        self.name = name
        self.jobs = {}
        self.facilities = {}
        self.taskQueue = task_queue

    def startJob(self, job):
        self.jobs[job.name] = job
        
    def removeJob(self, jobName):
        del self.jobs[jobName]
    
    def root(self):
        return WorkerRoot(self)
    
class Facility:
    
    def __init__(self, name, worker):
        self.name = name
        self.worker = worker
        
    def root(self):
        return FacilityRoot(self)
    
class Job:
    
    def __init__(self):
        self.name = str(uuid.uuid4())
        self.dateCreated = datetime.datetime.now()
        
    def execute(self):
        pass

class SleepJob(Job):
    
    def __init__(self):
        Job.__init__(self)
        
    def execute(self):
        time.sleep(5)
        
    
class SimpleWorker:
    
    def __init__(self, task_queue):    
        self.taskQueue = task_queue
        
    @cherrypy.expose        
    def index(self):
        return "Hello, world: I am a worker!"
    
    @cherrypy.expose
    def qsize(self):
        return str(self.taskQueue.qsize())
    
    @cherrypy.expose
    def sleep(self):
        if cherrypy.request.method == "GET":
            return "Why are you GETting me?"
        elif cherrypy.request.method == "POST":
            print "Got a POST..."
            job = SleepJob()
            self.taskQueue.put(job)
            return simplejson.dumps(job.name)
        else:
            return "Unusual method."

def server_main(task_queue):
    
    cherrypy.request.process_request_body = False
    cherrypy.quickstart(SimpleWorker(task_queue))