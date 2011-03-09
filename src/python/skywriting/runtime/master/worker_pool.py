# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from __future__ import with_statement
from cherrypy.process import plugins
from Queue import Queue
from threading import Condition, RLock
from skywriting.runtime.block_store import SWReferenceJSONEncoder
import datetime
import simplejson
import httplib2
import uuid
import random
import logging
import ciel
from skywriting.runtime.exceptions import WorkerFailedException

class FeatureQueues:
    def __init__(self):
        self.queues = {}
        self.streaming_queues = {}
        
    def get_queue_for_feature(self, feature):
        try:
            return self.queues[feature]
        except KeyError:
            queue = Queue()
            self.queues[feature] = queue
            return queue

    def get_streaming_queue_for_feature(self, feature):
        try:
            return self.streaming_queues[feature]
        except KeyError:
            queue = Queue()
            self.streaming_queues[feature] = queue
            return queue

class Worker:
    
    def __init__(self, worker_id, worker_descriptor, feature_queues, worker_pool):
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.last_ping = datetime.datetime.now()
        self.failed = False
        self.worker_pool = worker_pool
        self.assigned_tasks = set()
        
    def add_assigned_task(self, task):
        if self.failed:
            raise WorkerFailedException(self)
        self.assigned_tasks.add(task)
        
    def remove_assigned_task(self, task):
        self.assigned_tasks.remove(task)
        
    def get_assigned_tasks(self):
        return self.assigned_tasks.copy()

    def load(self):
        return len(self.assigned_tasks)

    def idle(self):
        pass

    def __repr__(self):
        return 'Worker(%s)' % self.id

    def as_descriptor(self):
        return {'worker_id': self.id,
                'netloc': self.netloc,
                'features': self.features,
                'last_ping': self.last_ping.ctime(),
                'failed':  self.failed}
        

class WorkerPool(plugins.SimplePlugin):
    
    def __init__(self, bus, deferred_worker):
        plugins.SimplePlugin.__init__(self, bus)
        self.deferred_worker = deferred_worker
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        self._lock = RLock()
        self.feature_queues = FeatureQueues()
        self.event_count = 0
        self.event_condvar = Condition(self._lock)
        self.max_concurrent_waiters = 5
        self.current_waiters = 0
        self.is_stopping = False        

    def subscribe(self):
        self.bus.subscribe('worker_failed', self.worker_failed)
        self.bus.subscribe('worker_ping', self.worker_ping)
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        
    def unsubscribe(self):
        self.bus.unsubscribe('worker_failed', self.worker_failed)
        self.bus.unsubscribe('worker_ping', self.worker_ping)
        self.bus.unsubscribe('start', self.start, 75)
        self.bus.unsubscribe('stop', self.server_stopping) 

    def start(self):
        self.deferred_worker.do_deferred_after(30.0, self.reap_dead_workers)
        
    def reset(self):
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        self.feature_queues = FeatureQueues()
        
    def allocate_worker_id(self):
        return str(uuid.uuid1())
        
    def create_worker(self, worker_descriptor):
        with self._lock:
            id = self.allocate_worker_id()
            worker = Worker(id, worker_descriptor, self.feature_queues, self)
            self.workers[id] = worker
            try:
                previous_worker_at_netloc = self.netlocs[worker.netloc]
                ciel.log.error('Worker at netloc %s has reappeared' % worker.netloc, 'WORKER_POOL', logging.WARNING)
                self.worker_failed(previous_worker_at_netloc)
            except KeyError:
                pass
            self.netlocs[worker.netloc] = worker
            self.idle_set.add(id)
            self.event_count += 1
            self.event_condvar.notify_all()
            
        try:
            has_blocks = worker_descriptor['has_blocks']
        except:
            has_blocks = False
            
        if has_blocks:
            ciel.log.error('%s has blocks, so will fetch' % str(worker), 'WORKER_POOL', logging.INFO)
            self.bus.publish('fetch_block_list', worker)
            
        self.bus.publish('schedule')
        return id
    
    def shutdown(self):
        for worker in self.workers.values():
            try:
                httplib2.Http().request('http://%s/control/kill/' % worker.netloc)
            except:
                pass
        
    def get_worker_by_id(self, id):
        with self._lock:
            return self.workers[id]
        
    def get_idle_workers(self):
        with self._lock:
            worker_list = map(lambda x: self.workers[x], self.idle_set)
        return worker_list
    
    def execute_task_on_worker(self, worker, task):
        try:
            worker.add_assigned_task(task)
            httplib2.Http().request("http://%s/control/task/" % (worker.netloc), "POST", simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder), )
        except:
            self.worker_failed(worker)
            
    def task_completed_on_worker(self, task, done_worker):
        for worker in task.get_workers():
            worker.remove_assigned_task(task)
            if worker is not done_worker:
                self.abort_task_on_worker(task, worker)
        
    def abort_task_on_worker(self, task, worker):
        try:
            print "Aborting task %d on worker %s" % (task.task_id, worker)
            httplib2.Http().request('http://%s/control/abort/%s/%s' % (worker.netloc, task.job.id, task.task_id), 'POST')
        except:
            self.worker_failed(worker)
    
    def worker_failed(self, worker):
        ciel.log.error('Worker failed: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING, True)
        with self._lock:
            failed_tasks = worker.get_assigned_tasks()
            worker.failed = True
            del self.netlocs[worker.netloc]
            del self.workers[worker.id]

        for failed_task in failed_tasks:
            failed_task.job.investigate_task_failure(failed_task, ('WORKER_FAILED', None, {}))
        
    def worker_ping(self, worker):
        with self._lock:
            self.event_count += 1
            self.event_condvar.notify_all()
        worker.last_ping = datetime.datetime.now()
        
    def get_all_workers(self):
        with self._lock:
            return self.workers.values()

    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            self.event_condvar.notify_all()

    def investigate_worker_failure(self, worker):
        ciel.log.error('Investigating possible failure of worker %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING)
        try:
            _, content = httplib2.Http().request('http://%s/control' % (worker.netloc, ), 'GET')
            id = simplejson.loads(content)
            assert id == worker.id
        except:
            self.bus.publish('worker_failed', worker)

    def get_random_worker(self):
        with self._lock:
            return random.choice(self.workers.values())
        
    def get_worker_at_netloc(self, netloc):
        try:
            return self.netlocs[netloc]
        except KeyError:
            return None

    def reap_dead_workers(self):
        if not self.is_stopping:
            for worker in self.workers.values():
                if worker.failed:
                    continue
                if (worker.last_ping + datetime.timedelta(seconds=30)) < datetime.datetime.now():
                    failed_worker = worker
                    self.deferred_worker.do_deferred(lambda: self.investigate_worker_failure(failed_worker))
                    
            self.deferred_worker.do_deferred_after(30.0, self.reap_dead_workers)
