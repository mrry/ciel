'''
Created on Apr 15, 2010

@author: derek
'''
from cherrypy.process import plugins
from Queue import Queue
from threading import Lock
import simplejson
import httplib2



class Worker:
    
    def __init__(self, worker_id, worker_descriptor):
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.current_task_id = None


class WorkerPool(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        
        self.idle_worker_queue = Queue()
        
        self.current_worker_id = 0
        self.workers = {}
        
        self.idle_set = set()
        
        self._lock = Lock()
        self.http = httplib2.Http()
        
    def subscribe(self):
        self.bus.subscribe('worker_failed', self.worker_failed)
        self.bus.subscribe('worker_idle', self.worker_idle)
        
    def unsubscribe(self):
        self.bus.unsubscribe('worker_failed', self.worker_failed)
        self.bus.unsubscribe('worker_idle', self.worker_idle)
        
    def create_worker(self, worker_descriptor):
        with self._lock:
            id = self.current_worker_id
            self.current_worker_id += 1
            worker = Worker(id, worker_descriptor)
            self.workers[id] = worker
        
    def get_worker_by_id(self, id):
        with self._lock:
            return self.workers[id]
        
    def get_idle_worker_ids(self):
        with self._lock:
            return list(self.idle_set)
    
    def execute_task_on_worker(self, worker, task):
        with self._lock:
            self.idle_set.remove(worker.id)
            worker.current_task_id = task.task_id
            task.worker_id = worker.id
    
        try:
            self.http.request("http://%s/task", "POST", simplejson.dumps(task.as_descriptor()))
        except:
            self.worker_failed(worker.id)
    
    def worker_failed(self, id):
        with self._lock:
            self.idle_set.remove(id)
            failed_task = self.workers[id].current_task_id

        if failed_task is not None:
            self.bus.publish('task_failed', failed_task, 'WORKER_FAILED')
        
    def worker_idle(self, id):
        with self._lock:
            self.workers[id].current_task_id = None
            self.idle_set.add(id)