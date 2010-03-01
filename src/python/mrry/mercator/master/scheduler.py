'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.process import plugins
from threading import Lock, Thread
from Queue import Queue, Empty
from uuid import uuid4
from mrry.mercator.jobmanager.plugins import THREAD_TERMINATOR
from mrry.mercator.master.datamodel import Session, Worker, TaskAttempt,\
    TASK_ATTEMPT_STATUS_FAILED, TASK_STATUS_QUEUED, TASK_STATUS_COMPLETED,\
    TASK_ATTEMPT_STATUS_COMPLETED, WORKER_STATUS_IDLE, TASK_STATUS_RUNNABLE,\
    WORKER_STATUS_BUSY, TASK_STATUS_RUNNING
from sqlalchemy.orm import eagerload
import simplejson
import httplib2
import collections

class SchedulerProxy(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)

        self.internal_scheduler = SingleThreadedScheduler(bus)
        
        self.input_queue = Queue()
        
        self.is_running = False

    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        
        self.bus.subscribe('add_worker', self.add_worker)
        self.bus.subscribe('remove_worker', self.remove_worker)
        self.bus.subscribe('add_task', self.add_task)
        self.bus.subscribe('add_failed_task_attempt', self.add_failed_task_attempt)
        self.bus.subscribe('add_completed_task_attempt', self.add_completed_task_attempt)
        
    def trigger_scheduler(self, reason):
        self.scheduler_invoke_queue.put(reason)
  
    def add_worker(self, worker):
        self.input_queue.put((self.internal_scheduler.add_worker, (worker, )))
        self.trigger_scheduler('new_worker')
        
    def remove_worker(self, worker_id):
        self.input_queue.put((self.internal_scheduler.remove_worker, (worker_id, )))
        self.trigger_scheduler('dead_worker')
  
    def add_task(self, task):
        self.input_queue.put((self.internal_scheduler.add_task, (task, )))
        self.trigger_scheduler('new_task')
        
    def add_failed_task_attempt(self, attempt_id):
        self.input_queue.put((self.internal_scheduler.add_failed_task_attempt, (attempt_id, )))
        self.trigger_scheduler('task_failed')
        
    def add_completed_task_attempt(self, attempt_id, new_output_data):
        self.input_queue.put((self.internal_scheduler.add_completed_task_attempt, (attempt_id, new_output_data)))
        self.trigger_scheduler('task_completed')
  
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.decision_thread = Thread(target=self.scheduler_main, args=())
            self.decision_thread.start()
        
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.scheduler_invoke_queue.put('shutdown')
            self.decision_thread.join()
  
    def scheduler_main(self):
        while True:
            reason = self.scheduler_invoke_queue.get()
            if not self.is_running or reason == 'shutdown':
                break
            self.do_schedule()
            
    def do_schedule(self):
        while True:
            try:
                method, args = self.input_queue.get_nowait()
                method(*args)
            except Empty:
                break

        self.internal_scheduler.do_schedule()
    
class SingleThreadedScheduler:
    
    def __init__(self, bus):
        
        self.bus = bus
        
        self.session = Session()
        
        # TODO: this should probably be a SQLite database.
        
        # Set of datum-id.
        self.available_data = set()
        
        # Set of worker-id.
        self.idle_workers = set()
        
        # Map from worker-id to task-id.
        self.busy_workers = {}
        
        # Map from task-id to task objects [(worker-id, production-rule) tuples].
        self.running_tasks = {}
    
        # List of production rules.
        self.runnable_list = []
        
        # List of (production rule, set of datum-id) tuples.
        self.blocked_list = []
    
    def add_worker(self, worker):
        self.session.add(worker)
        
    def remove_worker(self, worker_id):
        worker = self.session.query(Worker).get(worker_id)
        
        for attempt in worker.task_attempts:
            self.add_failed_task_attempt(attempt.id)
        
        self.session.delete(worker)
        
        # TODO: see if this worker was the only source for a bunch of inputs, if so, may have to reproduce them.
        # This may make a bunch of tasks non-runnable.
        # This will probably require notions of (i) data being produced (by running tasks), and (ii) storing the production
        # rule in the data store.
    
    def add_task(self, task):
        self.session.add(task)
#        
#        dependency_list = rule.get_data_dependencies()
#        pending_dependency_list = [x for x in dependency_list if x not in self.available_data]
#        if len(pending_dependency_list == 0):
#            self.runnable_list.append(rule)
#        else:
#            self.blocked_list.append((rule, set(pending_dependency_list)))
    
    def add_failed_task_attempt(self, task_attempt_id):
        attempt = self.session.query(TaskAttempt).get(task_attempt_id)
        task = attempt.task
        
        attempt.status = TASK_ATTEMPT_STATUS_FAILED
        task.status = TASK_STATUS_QUEUED
        
    def add_completed_task_attempt(self, task_attempt_id, new_output_data):
        attempt = self.session.query(TaskAttempt).get(task_attempt_id)
        task = attempt.task
        worker = attempt.worker
        
        attempt.status = TASK_ATTEMPT_STATUS_COMPLETED
        task.status = TASK_STATUS_COMPLETED
        worker.status = WORKER_STATUS_IDLE
        
        for new_output_datum in new_output_data:
            self.session.add(new_output_datum)
        
#        task = self.running_tasks[task_id]
#        del self.running_tasks[task_id]
#        del self.busy_workers[task.worker]
#        self.idle_workers.add(task.worker)
#        
#        newly_available_data = task.rule.get_data_outputs()
#        
#        # TODO: replace with a smarter (tree-based) lookup structure for the
#        #       pending-dependency set.
#        #       (e.g. a map from pending output to sets).
#        still_blocked_list = []
#        for (blocked_rule, pending_dependency_set) in self.blocked_list:
#            for newly_available_output in newly_available_data:
#                pending_dependency_set.discard(newly_available_output)
#            
#            if len(pending_dependency_set) == 0:
#                self.runnable_list.append(blocked_rule)
#            else:
#                still_blocked_list.append(blocked_rule, pending_dependency_set)
#                
#        self.blocked_list = still_blocked_list    
#    
#        for newly_available_output in newly_available_data:
#            self.available_data.add(newly_available_output)
    
    def assign_task(self, attempt):
        self.idle_workers.remove(task.worker)
        self.busy_workers[task.worker] = task.id
        self.running_tasks[task.id] = task
        
    
    def do_schedule(self):
        self.session.flush()
        idle_workers = self.session.query(Worker).filter_by(status=WORKER_STATUS_IDLE)
        runnable_tasks = self.session.query(Task).filter_by(status=TASK_STATUS_RUNNABLE)
        while not len(self.idle_workers) == 0 or len(self.runnable_list) == 0:
            # Really we should be doing Quincy or something locality-aware in here, but instead let's just do an arbitrary mapping.
            task = runnable_tasks.pop()
            worker = idle_workers.pop()
            attempt = TaskAttempt(task=task, worker=worker)
            worker.status = WORKER_STATUS_BUSY
            task.status = TASK_STATUS_RUNNING
            self.session.add(attempt)
            self.session.flush()
    
            self.bus.publish('execute_task_attempt', attempt.id)
        
class TaskExecutor(plugins.SimplePlugin):
    
    def __init__(self, bus, worker_pool):
        plugins.SimplePlugin.__init__(self, bus)
        self.is_running = False
        self.task_queue = Queue()
        self.worker_pool = worker_pool
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('execute_task', self.execute_task)
        
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = Thread(target=self.executor_main, args=())
            self.thread.start()
            
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.task_queue.put(THREAD_TERMINATOR)
            self.thread.join()
            
    def execute_task_attempt(self, task_attempt_id):
        self.task_queue.put(task_attempt_id)
    
    def executor_main(self):
        http = httplib2.Http()
        self.session = Session()
        while True:
            task_attempt_id = self.task_queue.get()
            if task_attempt_id is THREAD_TERMINATOR or not self.is_running:
                break
            
            task_attempt = self.session.query(TaskAttempt).options(eagerload('task')).get(task_attempt_id)
            
            # Task dispatch code.
            
            # Get URL from worker pool.
            worker_url = task_attempt.worker.uri
            
            method = task_attempt.task.method
            args = task_attempt.task.args
            
            inputs = task_attempt.task.inputs
            outputs = task_attempt.task.outputs
            
            arg_representations = {}
            for input in inputs:
                # Need to get representation as a dict.
                arg_representations[input.id] = map(lambda x: x.as_dict(), input.representations)
            
            processed_outputs = []
            for output in outputs:
                processed_outputs.append(output.id)
            
            # Issue task creation request to worker URL.
            request_body = simplejson.dumps({ 'master_task_id' : task_attempt.task.id,
                                              'master_task_attempt_id' : task_attempt.id,
                                              'executor' : task_attempt.task.method,
                                              'args' : args,
                                              'arg_representations' : arg_representations,
                                              'outputs' : processed_outputs })  
                      
            (response, content) = http.request(worker_url, 'POST', request_body)
            
            # If failed, create a failed task event!
            if response.status != 200:
                self.bus.publish('add_failed_task_attempt', task_attempt.id)

        self.session.close()
        

        
