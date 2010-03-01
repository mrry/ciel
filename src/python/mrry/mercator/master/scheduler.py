'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.process import plugins
from threading import Lock, Thread
from Queue import Queue, Empty
from uuid import uuid4
from mrry.mercator.jobmanager.plugins import THREAD_TERMINATOR
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
        self.bus.subscribe('add_production_rule', self.add_production_rule)
        self.bus.subscribe('add_failed_task', self.add_failed_task)
        self.bus.subscribe('add_completed_task', self.add_completed_task)
        
    def trigger_scheduler(self, reason):
        self.scheduler_invoke_queue.put(reason)
  
    def add_worker(self, worker_id):
        self.new_worker_queue.put((self.internal_scheduler.add_worker, worker_id))
        self.trigger_scheduler('new_worker')
        
    def remove_worker(self, worker_id):
        self.dead_worker_queue.put((self.internal_scheduler.remove_worker, worker_id))
        self.trigger_scheduler('dead_worker')
  
    def add_production_rule(self, rule):
        self.production_rule_queue.put((self.internal_scheduler.add_production_rule, rule))
        self.trigger_scheduler('new_task')
        
    def add_failed_task(self, task_id):
        self.failed_task_queue.put((self.internal_scheduler.add_failed_task, task_id))
        self.trigger_scheduler('task_failed')
        
    def add_completed_task(self, task_id):
        self.completed_task_queue.put((self.internal_scheduler.add_completed_task, task_id))
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
                new_worker = self.new_worker_queue.get_nowait()
                self.internal_scheduler.add_worker(new_worker)
            except Empty:
                break

        self.internal_scheduler.do_schedule()
    
class SingleThreadedScheduler:
    
    def __init__(self, bus):
        
        self.bus = bus
        
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
    
    def add_worker(self, worker_id):
        self.idle_workers.add(worker_id)
        
    def remove_worker(self, worker_id):
        try:
            failed_task_id = self.busy_workers[worker_id]
            del self.busy_workers[worker_id]
            self.add_failed_task(failed_task_id)
        except KeyError:
            return
        
        # TODO: see if this worker was the only source for a bunch of inputs, if so, may have to reproduce them.
        # This may make a bunch of tasks non-runnable.
        # This will probably require notions of (i) data being produced (by running tasks), and (ii) storing the production
        # rule in the data store.
    
    def add_production_rule(self, rule):
        dependency_list = rule.get_data_dependencies()
        pending_dependency_list = [x for x in dependency_list if x not in self.available_data]
        if len(pending_dependency_list == 0):
            self.runnable_list.append(rule)
        else:
            self.blocked_list.append((rule, set(pending_dependency_list)))
    
    def add_failed_task(self, task_id):
        task = self.running_tasks[task_id]
        del self.running_tasks[task_id]
        self.add_production_rule(task.rule)
        
    def add_completed_task(self, task_id):
        task = self.running_tasks[task_id]
        del self.running_tasks[task_id]
        del self.busy_workers[task.worker]
        self.idle_workers.add(task.worker)
        
        newly_available_data = task.rule.get_data_outputs()
        
        # TODO: replace with a smarter (tree-based) lookup structure for the
        #       pending-dependency set.
        #       (e.g. a map from pending output to sets).
        still_blocked_list = []
        for (blocked_rule, pending_dependency_set) in self.blocked_list:
            for newly_available_output in newly_available_data:
                pending_dependency_set.discard(newly_available_output)
            
            if len(pending_dependency_set) == 0:
                self.runnable_list.append(blocked_rule)
            else:
                still_blocked_list.append(blocked_rule, pending_dependency_set)
                
        self.blocked_list = still_blocked_list    
    
        for newly_available_output in newly_available_data:
            self.available_data.add(newly_available_output)
    
    def assign_task(self, task):
        self.idle_workers.remove(task.worker)
        self.busy_workers[task.worker] = task.id
        self.running_tasks[task.id] = task
        
        self.bus.publish('execute_task', task)
    
    def do_schedule(self):
        while not len(self.idle_workers) == 0 or len(self.runnable_list) == 0:
            # Really we should be doing Quincy or something locality-aware in here, but instead let's just do an arbitrary mapping.
            rule = self.runnable_list.pop()
            worker = self.idle_workers.pop()
            task = WorkflowTask(rule, worker)
            self.assign_task(task)
            
class WorkflowTask:
    
    def __init__(self, rule, worker):
        self.id = uuid4()
        self.rule = rule
        self.worker = worker
        
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
            
    def execute_task(self, task):
        self.task_queue.put(task)
    
    def executor_main(self):
        http = httplib2.Http()
        while True:
            task = self.task_queue.get()
            if task is THREAD_TERMINATOR or not self.is_running:
                break
            
            # Task dispatch code.
            
            # Get URL from worker pool.
            worker_url = self.worker_pool.get_task_submission_url(task.worker)
            
            # Issue task creation request to worker URL.
            request_body = simplejson.dumps({ 'master_task_id' : task.id,
                                              'executor' : task.rule.method,
                                              'args' : task.rule.get_args_for_submit(), 
                                              'outputs' : task.rule.get_outputs_for_submit() })  
                      
            (response, content) = http.request(worker_url, 'POST', request_body)
            
            # If failed, create a failed task event!
            if response.status != 200:
                self.bus.publish('add_failed_task', task.id)
            
class WorkerPool(plugins.SimplePlugin):
    
    def __init__(self):
        # Map from worker-id to worker object.
        self.workers = {}
        
