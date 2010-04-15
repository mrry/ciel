'''
Created on 4 Feb 2010

@author: dgm36
'''

from cherrypy.process import plugins
from Queue import Queue, Empty
from threading import Lock
from urlparse import urljoin
import simplejson
import threading
import httplib2

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class StatusMaintainer(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self.job_statuses = {}
        self.job_statuses_lock = Lock()
    
    def subscribe(self):
        self.bus.subscribe('update_status', self.update_status)
    
    def list_jobs(self):
        with self.job_statuses_lock:
            jobs = list(self.job_statuses.keys())
        return jobs
    
    def update_status(self, job_id, status):
        with self.job_statuses_lock:
            self.job_statuses[job_id] = status
            
    def get_status(self, job_id):
        with self.job_statuses_lock:
            try:
                status = self.job_statuses[job_id]
            except KeyError:
                status = None
        return status
    
class Pinger(plugins.SimplePlugin):
    
    def __init__(self, bus, master_uri, name):
        plugins.SimplePlugin.__init__(self, bus)
        self.queue = Queue()
        self.non_urgent_queue = Queue()
        self.ping_uri = urljoin(master_uri, 'ping/')
        self.name = name
        self.thread = None
        self.is_running = False
                
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('ping_master', self.ping_master)
        self.bus.subscribe('ping_non_urgent', self.ping_non_urgent)
        
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self.thread_main, args=())
            self.thread.start()
    
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.queue.put(THREAD_TERMINATOR)
            self.thread.join()
    
    def ping_master(self, message):
        self.queue.put(message)
        
    def ping_non_urgent(self, message):
        self.non_urgent_queue.put(message)
        
    def thread_main(self):
        http = httplib2.Http()
        while True:
            
            update = []
            
            try:
                new_thing = self.queue.get(block=True, timeout=30)
                if not self.is_running or update is THREAD_TERMINATOR:
                    update.append(('worker', 'TERMINATING'))
                else:
                    update.append(new_thing)
            except Empty:
                pass
            
            if self.is_running:
                update.append(('worker', 'HEARTBEAT'))
                
                try:
                    while True:
                        update.append(self.queue.get_nowait())
                except Empty:
                    pass
                
                try:
                    while True:
                        update.append(self.non_urgent_queue.get_nowait())
                except Empty:
                    pass
            
            http.request(uri=self.ping_uri, method='POST', body=simplejson.dumps((self.name, update)))
            
            if not self.is_running:
                break
