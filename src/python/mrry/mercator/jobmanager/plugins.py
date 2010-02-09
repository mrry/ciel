'''
Created on 4 Feb 2010

@author: dgm36
'''

from cherrypy.process import plugins
from Queue import Queue, Empty
from threading import Lock
import struct
import simplejson
import subprocess
import threading
import httplib2

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class JobRunner(plugins.SimplePlugin):
    
    def __init__(self, bus, pool_size=5):
        plugins.SimplePlugin.__init__(self, bus)
        self.pool_size = pool_size
        self.queue = Queue()
        self.threads = []
        self.running_processes_lock = Lock()
        self.running_processes = set()
        self.is_running = False
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('create_job', self.create_job)
        
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
            for process in self.running_processes:
                process.kill()
        for thread in self.threads:
            thread.join()
        self.threads = []
    
    def create_job(self, job):
        self.bus.publish('update_status', job.id, "QUEUED")
        self.queue.put(job)
    
    def send_to_process(self, job_id, message):
        with self.running_processes_lock:
            stdin = self.running_processes[job_id].stdin
            stdin.write(struct.pack("I", len(message)))
            stdin.write(message)
            stdin.flush()
        
    
    def thread_main(self):
        
        while True:
            if not self.is_running:
                break
            job = self.queue.get()
            if job is THREAD_TERMINATOR:
                break
            
            self.bus.publish('update_status', job.id, "RUNNING")
            
            stdout_file = open("%s.out" % (job.id, ), "w")
            stderr_file = open("%s.err" % (job.id, ), "w")
            
            proc = subprocess.Popen(args=job.args, close_fds=True, stdin=subprocess.PIPE, stdout=stdout_file, stderr=stderr_file)
    
            
            with self.running_processes_lock:
                self.running_processes.add(proc)
                        
            sizeof_uint = struct.calcsize("I")
            
            sync_terminate = False
            
            while True:
                length_str = proc.stdout.read(sizeof_uint)
            
                if length_str == "":
                    # Reached EOF without receiving a zero-length string. This means that
                    # the process has died and we should not try to write to it. Should
                    # really separate out the communicable-vs-non-communicable job 
                    # processes.
                    sync_terminate = False
                    break
                    
                length = struct.unpack("I", length_str)
                
                # The final message from the process will be zero-length.
                if length == 0:
                    sync_terminate = True
                    break
                
                message = proc.stdout.read(length)
                self.bus.publish('update_status', job.id, ("RUNNING", message))
                
            with self.running_processes_lock:
                self.running_processes.remove(proc)
                
            if sync_terminate:
                # Finally send a zero-length message to the process to acknowledge that
                # it is terminated. At this point, we know that we will not try to send
                # any more messages to the process.
                proc.stdin.write(struct.pack("I", 0))
                proc.stdin.flush()
            
            rc = proc.wait()
            
            with self.running_processes_lock:
                self.running_processes.remove(proc)
            
            self.bus.publish('update_status', job.id, ("TERMINATED", rc))
            
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
    
    def __init__(self, bus, target, name):
        plugins.SimplePlugin.__init__(self, bus)
        self.queue = Queue()
        self.target = target
        self.name = name
        self.thread = None
                
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('update_status', self.update_status)
        
    def start(self):
        self.thread = threading.Thread(target=self.thread_main, args=())
        self.thread.start()
    
    def stop(self):
        self.queue.put(THREAD_TERMINATOR)
        if self.thread is not None:
            self.thread.join()
    
    def update_status(self, job_id, status):
        http = httplib2.Http()
        while True:
            try:
                update = self.queue.get(block=False, timeout=30)
                if update is THREAD_TERMINATOR:
                    update = ("PINGER_TERMINATING")
            except Empty:
                update = ("HEARTBEAT")
            
            http.request(url=self.target, method='POST', body=simplejson.dumps((self.name, update)))
    def thread_main(self):
        pass