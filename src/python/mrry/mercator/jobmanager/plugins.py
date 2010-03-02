'''
Created on 4 Feb 2010

@author: dgm36
'''

from cherrypy.process import plugins
from Queue import Queue, Empty
from threading import Lock
from urlparse import urljoin
import struct
import simplejson
import subprocess
import threading
import httplib2

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class JobRunner(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)

    def subscribe(self):
        self.bus.subscribe('create_job', self.create_job)
        self.bus.subscribe('input_fetched', self.input_fetched)
        self.bus.subscribe('input_failed', self.input_failed)
        self.bus.subscribe('job_completed', self.job_completed)

    def create_job(self, job):
        if input.job.is_runnable():
            self.bus.publish('execute_job', input.job)
        else:
            self.bus.publish('update_status', job.id, "WAITING_FOR_INPUTS")
            with job._lock:
                for input in job.args.pending_inputs:
                    self.bus.publish('fetch_input', input)
    
    def input_fetched(self, input, filename):
        input.job.set_input_filename(input.id, filename)
        if input.job.is_runnable():
            self.bus.publish('execute_job', input.job)
    
    def input_failed(self, input, filename):
        # Should probably keep a failure count and kill job if necessary (>threshold).
        pass

    def job_completed(self, job, return_value):
        pass

class InputFetcher(plugins.SimplePlugin):
    
    def __init__(self, bus, pool_size=5):
        plugins.SimplePlugin.__init__(self, bus)
        self.pool_size = pool_size
        self.queue = Queue()
        self.threads = []
        self.is_running = False
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('fetch_input', self.fetch_input)
        
    def start(self):
        self.is_running = True
        for i in range(self.pool_size):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
        
    def stop(self):
        self.is_running = False
        for i in range(self.pool_size):
            self.queue.put(THREAD_TERMINATOR)
        for thread in self.threads:
            thread.join()
        self.threads = []
        
    def fetch_input(self, input):
        self.queue.put(input)

    def thread_main(self):
        
        while True:
            if not self.is_running:
                break
            input = self.queue.get()
            if input is THREAD_TERMINATOR:
                break

            try:
                filename = input.fetch()
                self.bus.publish('input_fetched', input, filename)
            except Exception as ex:
                self.bus.publish('input_failed', input, ex)
            
class JobExecutor(plugins.SimplePlugin):
    
    def __init__(self, bus, pool_size=5):
        plugins.SimplePlugin.__init__(self, bus)
        self.pool_size = pool_size
        self.queue = Queue()
        self.threads = []
        self.running_processes_lock = Lock()
        self.running_processes = {}
        self.is_running = False
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('execute_job', self.execute_job)
        
    def start(self):
        self.is_running = True
        for i in range(self.pool_size):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
     
    def stop(self):
        self.is_running = False
        for i in range(self.pool_size):
            self.queue.put(THREAD_TERMINATOR)
        with self.running_processes_lock:
            for running_job in self.running_processes.values():
                running_job.kill()
        for thread in self.threads:
            thread.join()
        self.threads = []
    
    def execute_job(self, job):
        self.bus.publish('update_status', job.id, "QUEUED")
        self.queue.put(job)
    
    def send_to_process(self, job_id, message):
        with self.running_processes_lock:
            job = self.running_processes[job_id]
        job.send_message(message)
    
    def thread_main(self):
        
        while True:
            if not self.is_running:
                break
            job = self.queue.get()
            if job is THREAD_TERMINATOR:
                break
            
            with self.running_processes_lock:
                self.running_processes[job.id] = job
                
            ret = job.run(self.bus)
            
            with self.running_processes_lock:
                del self.running_processes[job.id]
            
            self.bus.publish('job_completed', job, ret)
            
            
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
                update.append(self.queue.get(block=True, timeout=30))
                if not self.is_running or update is THREAD_TERMINATOR:
                    update.append(('worker', 'TERMINATING'))
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
            
class DataManager(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self.data_dict = {}
        self._lock = Lock()
        
    def subscribe(self):
        self.bus.subscribe('publish_static_file', self.publish_static_file)
        
    def publish_static_file(self, datum_id, filename):
        with self._lock:
            self.data_dict[datum_id] = filename
            
    def get_filename(self, datum_id):
        with self._lock:
            return self.data_dict[datum_id]
        
    def get_all_data(self):
        with self._lock:
            return list(self.data_dict.keys())