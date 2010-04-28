'''
Created on Apr 15, 2010

@author: derek
'''
from __future__ import with_statement
from cherrypy.process import plugins
from Queue import Queue
from threading import Lock
import random
import datetime
import sys
import simplejson
import httplib2



class Worker:
    
    def __init__(self, worker_id, worker_descriptor):
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.current_task_id = None
        self.last_ping = datetime.datetime.now()

    def as_descriptor(self):
        return {'worker_id': self.id,
                'netloc': self.netloc,
                'features': self.features,
                'current_task_id': self.current_task_id,
                'last_ping': self.last_ping.ctime()}

class WorkerPool(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self.idle_worker_queue = Queue()
        self.current_worker_id = 0
        self.workers = {}
        self.idle_set = set()
        self._lock = Lock()
        
    def subscribe(self):
        self.bus.subscribe('worker_failed', self.worker_failed)
        self.bus.subscribe('worker_idle', self.worker_idle)
        self.bus.subscribe('worker_ping', self.worker_ping)
        
    def unsubscribe(self):
        self.bus.unsubscribe('worker_failed', self.worker_failed)
        self.bus.unsubscribe('worker_idle', self.worker_idle)
        self.bus.unsubscribe('worker_ping', self.worker_ping)
        
    def create_worker(self, worker_descriptor):
        with self._lock:
            id = self.current_worker_id
            self.current_worker_id += 1
            worker = Worker(id, worker_descriptor)
            self.workers[id] = worker
            self.idle_set.add(id)
        self.bus.publish('schedule')
        return id
        
    def get_worker_by_id(self, id):
        with self._lock:
            return self.workers[id]
        
    def get_idle_worker_ids(self):
        with self._lock:
            return list(self.idle_set)
    
    def execute_task_on_worker_id(self, worker_id, task):
        with self._lock:
            self.idle_set.remove(worker_id)
            worker = self.workers[worker_id]
            worker.current_task_id = task.task_id
            task.worker_id = worker_id
            
        try:
            print "Assigning task:", task.as_descriptor()
            httplib2.Http().request("http://%s/task/" % (worker.netloc), "POST", simplejson.dumps(task.as_descriptor()), )
        except:
            print sys.exc_info()
            print 'Worker failed:', worker_id
            self.worker_failed(worker_id)
    
    def worker_failed(self, id):
        with self._lock:
            self.idle_set.discard(id)
            failed_task = self.workers[id].current_task_id

        if failed_task is not None:
            self.bus.publish('task_failed', failed_task, 'WORKER_FAILED')
        
    def worker_idle(self, id):
        with self._lock:
            self.workers[id].current_task_id = None
            self.idle_set.add(id)
        self.bus.publish('schedule')
            
    def worker_ping(self, id, status, ping_news):
        with self._lock:
            worker = self.workers[id]
        worker.last_ping = datetime.datetime.now()
        
    def get_all_workers(self):
        with self._lock:
            return map(lambda x: x.as_descriptor(), self.workers.values())

    def get_random_worker(self):
        with self._lock:
            return random.choice(self.workers.values())