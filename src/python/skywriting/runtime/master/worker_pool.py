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
import cherrypy
import random
import logging

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
    
    def __init__(self, worker_id, worker_descriptor, feature_queues):
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.current_task = None
        self.last_ping = datetime.datetime.now()
        
        self.failed = False
        
        self.local_queue = Queue()
        self.queues = [self.local_queue]
        for feature in self.features:
            self.queues.append(feature_queues.get_queue_for_feature(feature))
        for feature in self.features:
            self.queues.append(feature_queues.get_streaming_queue_for_feature(feature))

    def __repr__(self):
        return 'Worker(%s)' % self.id

    def as_descriptor(self):
        return {'worker_id': self.id,
                'netloc': self.netloc,
                'features': self.features,
                'current_task_id': self.current_task.task_id if self.current_task is not None else None,
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
        self.bus.subscribe('worker_idle', self.worker_idle)
        self.bus.subscribe('worker_ping', self.worker_ping)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        self.deferred_worker.do_deferred_after(30.0, self.reap_dead_workers)
        
    def unsubscribe(self):
        self.bus.unsubscribe('worker_failed', self.worker_failed)
        self.bus.unsubscribe('worker_idle', self.worker_idle)
        self.bus.unsubscribe('worker_ping', self.worker_ping)
        self.bus.unsubscribe('stop', self.server_stopping) 
        
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
            worker = Worker(id, worker_descriptor, self.feature_queues)
            self.workers[id] = worker
            try:
                previous_worker_at_netloc = self.netlocs[worker.netloc]
                cherrypy.log.error('Worker at netloc %s has reappeared' % worker.netloc, 'WORKER_POOL', logging.WARNING)
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
            cherrypy.log.error('%s has blocks, so will fetch' % str(worker), 'WORKER_POOL', logging.INFO)
            self.bus.publish('fetch_block_list', worker)
            
        self.bus.publish('schedule')
        return id
    
    def shutdown(self):
        for worker in self.workers.values():
            try:
                httplib2.Http().request('http://%s/kill/' % worker.netloc)
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
        with self._lock:
            self.idle_set.remove(worker.id)
            worker.current_task = task
            task.set_assigned_to_worker(worker)
            self.event_count += 1
            self.event_condvar.notify_all()
            
        try:
            httplib2.Http().request("http://%s/task/" % (worker.netloc), "POST", simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder), )
        except:
            self.worker_failed(worker)
        
    def notify_task_streams_done(self, worker, task):
        try:
            httplib2.Http().request("http://%s/task/%s/streams_done" % (worker.netloc, task.task_id), "POST", "done")
        except:
            pass

    def abort_task_on_worker(self, task):
        worker = task.worker
    
        try:
            print "Aborting task %d on worker %s" % (task.task_id, worker)
            response, _ = httplib2.Http().request('http://%s/task/%d/abort' % (worker.netloc, task.task_id), 'POST')
            if response.status == 200:
                self.worker_idle(worker)
            else:
                print response
                print 'Worker failed to abort a task:', worker 
                self.worker_failed(worker)
        except:
            self.worker_failed(worker)
    
    def worker_failed(self, worker):
        cherrypy.log.error('Worker failed: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING)
        with self._lock:
            self.event_count += 1
            self.event_condvar.notify_all()
            self.idle_set.discard(worker.id)
            failed_task = worker.current_task
            worker.failed = True
            del self.netlocs[worker.netloc]
            del self.workers[worker.id]

        if failed_task is not None:
            self.bus.publish('task_failed', failed_task, ('WORKER_FAILED', None, {}))
        
    def worker_idle(self, worker):
        with self._lock:
            worker.current_task = None
            self.idle_set.add(worker.id)
            self.event_count += 1
            self.event_condvar.notify_all()
        self.bus.publish('schedule')
            
    def worker_ping(self, worker):
        with self._lock:
            self.event_count += 1
            self.event_condvar.notify_all()
        worker.last_ping = datetime.datetime.now()
        
    def get_all_workers(self):
        with self._lock:
            return self.workers.values()

    def get_all_workers_with_version(self):
        with self._lock:
            return (self.event_count, map(lambda x: x.as_descriptor(), self.workers.values()))

    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            self.event_condvar.notify_all()

    def await_version_after(self, target):
        with self._lock:
            self.current_waiters = self.current_waiters + 1
            while self.event_count <= target:
                if self.current_waiters > self.max_concurrent_waiters:
                    break
                elif self.is_stopping:
                    break
                else:
                    self.event_condvar.wait()
            self.current_waiters = self.current_waiters - 1
            if self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")
            elif self.is_stopping:
                raise Exception("Server stopping")
            return self.event_count

    def investigate_worker_failure(self, worker):
        cherrypy.log.error('Investigating possible failure of worker %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING)
        try:
            _, content = httplib2.Http().request('http://%s/' % (worker.netloc, ), 'GET')
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
