'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.process import plugins
from threading import Lock
import collections

class Scheduler(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self.workers = {}
        self.idle_workers = set()
        self.runnable_jobs = collections.deque()
        self.running_jobs = {}
        
    def subscribe(self):
        pass
        
    def add_runnable_job(self, job_details, callback):
        job_details.callback = callback
        self.runnable_jobs.append(job_details)
    
    def schedule(self):
        # For each idle worker, assign a runnable job.
        pass