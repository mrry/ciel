# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
import tempfile
import os
import uuid
import ciel
import logging
import simplejson
from datetime import datetime
from shared.references import SW2_FixedReference
import urlparse
from shared.references import SWReferenceJSONEncoder
import pickle
import threading
from skywriting.runtime.pycurl_rpc import post_string
from skywriting.runtime.executor_helpers import write_fixed_ref_string
from skywriting.runtime.block_store import is_ref_local
from shared.io_helpers import write_framed_json

class ProcessRecord:
    """Represents a long-running process that is attached to this worker."""
    
    def __init__(self, id, pid, protocol):
        self.id = id
        self.pid = pid
        self.protocol = protocol
        self.job_id = None
        self.is_free = False
        self.last_used_time = None
        
        # The worker communicates with the process using a pair of named pipes.
        self.fifos_dir = tempfile.mkdtemp(prefix="ciel-ipc-fifos-")
                
        self.from_process_fifo_name = os.path.join(self.fifos_dir, 'from_process')
        os.mkfifo(self.from_process_fifo_name)
        self.from_process_fifo = None 
        
        self.to_process_fifo_name = os.path.join(self.fifos_dir, 'to_process')
        os.mkfifo(self.to_process_fifo_name)
        self.to_process_fifo = None
        
        self._lock = threading.Lock()
        
    def set_pid(self, pid):
        self.pid = pid
        
    def get_read_fifo(self):
        with self._lock:
            if self.from_process_fifo is None:
                self.from_process_fifo = open(self.from_process_fifo_name, "r")
            return self.from_process_fifo
        
    def get_read_fifo_name(self):
        return self.from_process_fifo_name
        
    def get_write_fifo(self):
        with self._lock:
            if self.to_process_fifo is None:
                self.to_process_fifo = open(self.to_process_fifo_name, "w")
            return self.to_process_fifo
        
    def get_write_fifo_name(self):
        return self.to_process_fifo_name
        
    def cleanup(self):
        try:
            if self.from_process_fifo is not None: 
                self.from_process_fifo.close()
            if self.to_process_fifo is not None:
                self.to_process_fifo.close()
        except:
            ciel.log('Error cleaning up process %s, ignoring' % self.id, 'PROCESS', logging.WARN)
            
    def kill(self):
        ciel.log("Garbage collecting process %s" % self.id, "PROCESSPOOL", logging.INFO)
        write_fp = self.get_write_fifo()
        write_framed_json(("die", {"reason": "Garbage collected"}), write_fp)
        self.cleanup()
        
    def as_descriptor(self):
        return {'id' : self.id,
                'protocol' : self.protocol,
                'to_worker_fifo' : self.from_process_fifo_name,
                'from_worker_fifo' : self.to_process_fifo_name,
                'job_id' : self.job_id}
        
    def __repr__(self):
        return 'ProcessRecord(%s, %s)' % (repr(self.id), repr(self.pid))

class ProcessPool:
    """Represents a collection of long-running processes attached to this worker."""
    
    def __init__(self, bus, worker, soft_cache_executors):
        self.processes = {}
        self.bus = bus
        self.worker = worker
        self.lock = threading.Lock()
        self.gc_thread = None
        self.gc_thread_stop = threading.Event()
        self.soft_cache_executors = soft_cache_executors
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        
    def soft_cache_process(self, proc_rec, exec_cls):
        with self.lock:
            ciel.log("Caching process %s" % proc_rec.id, "PROCESSPOOL", logging.INFO)
            exec_cls.process_cache.append(proc_rec)
            proc_rec.is_free = True
            proc_rec.last_used_time = datetime.now()
    
    def get_soft_cache_process(self, exec_cls):
        with self.lock:
            try:
                proc = exec_cls.process_cache.pop()
                ciel.log("Re-using process %s" % proc.id, "PROCESSPOOL", logging.INFO)
                proc.is_free = False
                return proc
            except IndexError:
                return None
        
    def garbage_thread(self):
        while True:
            now = datetime.now()
            with self.lock:
                for executor in self.soft_cache_executors:
                    while len(executor.process_cache) > 0:
                        proc_rec = executor.process_cache[-1]
                        time_since_last_use = now - proc_rec.last_used_time
                        print proc_rec.last_used_time, now, time_since_last_use
                        if time_since_last_use.seconds > 30:
                            proc_rec.kill()
                            executor.process_cache.pop()
                        else:
                            break
            self.gc_thread_stop.wait(60)
            if self.gc_thread_stop.isSet():
                with self.lock:
                    for executor in self.soft_cache_executors:
                        for proc_rec in executor.process_cache:
                            try:
                                proc_rec.kill()
                            except Exception as e:
                                ciel.log("Failed to shut a process down (%s)" % repr(e), "PROCESSPOOL", logging.WARNING)
                ciel.log("Process pool garbage collector: terminating", "PROCESSPOOL", logging.INFO)
                return
        
    def start(self):
        self.gc_thread = threading.Thread(target=self.garbage_thread)
        self.gc_thread.start()
    
    def stop(self):
        self.gc_thread_stop.set()
        for record in self.processes.values():
            record.cleanup()
        
    def get_reference_for_process(self, record):
        ref = SW2_FixedReference(record.id, self.worker.block_store.netloc)
        if not is_ref_local(ref):
            write_fixed_ref_string(pickle.dumps(record.as_descriptor()), ref)
        return ref
        
    def create_job_for_process(self, record):
        ref = self.get_reference_for_process(record)
        root_task_descriptor = {'handler' : 'proc',
                                'dependencies' : [ref],
                                'task_private' : ref}
        
        master_task_submit_uri = urlparse.urljoin(self.worker.master_url, "control/job/")
        
        try:
            message = simplejson.dumps(root_task_descriptor, cls=SWReferenceJSONEncoder)
            content = post_string(master_task_submit_uri, message)
        except Exception, e:
            ciel.log('Network error submitting process job to master', 'PROCESSPOOL', logging.WARN)
            raise e

        job_descriptor = simplejson.loads(content)
        record.job_id = job_descriptor['job_id']
        ciel.log('Created job %s for process %s (PID=%d)' % (record.job_id, record.id, record.pid), 'PROCESSPOOL', logging.INFO)
        
        
    def create_process_record(self, pid, protocol, id=None):
        if id is None:
            id = str(uuid.uuid4())
        
        record = ProcessRecord(id, pid, protocol)
        self.processes[id] = record
        return record
        
    def get_process_record(self, id):
        try:
            return self.processes[id]
        except KeyError:
            return None
        
    def delete_process_record(self, record):
        del self.processes[record.id]
        record.cleanup()
        
    def get_process_ids(self):
        return self.processes.keys()
