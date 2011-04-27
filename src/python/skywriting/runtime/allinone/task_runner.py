# Copyright (c) 2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from skywriting.runtime.task_graph import DynamicTaskGraph, TaskGraphUpdate
import threading
from skywriting.runtime.executors import ExecutionFeatures
from skywriting.runtime.task import build_taskpool_task_from_descriptor,\
    TASK_COMMITTED
import ciel
import logging
import multiprocessing
from skywriting.runtime.block_store import BlockStore
import sys
from skywriting.runtime.task_executor import TaskExecutorPlugin

ACTION_SPAWN = 0
ACTION_PUBLISH = 1
ACTION_COMPLETE = 2
ACTION_STOP = 3
ACTION_REPORT = 4

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class AllInOneJobOutput:
    
    def __init__(self):
        self.event = threading.Event()
        self.result = None
   
    def is_queued_streaming(self):
        return False
    def is_blocked(self):
        return True
   
    def join(self):
        self.event.wait()
        return self.result
    
    def notify_ref_table_updated(self, ref_table_entry):
        self.result = ref_table_entry.ref
        self.event.set()

class AllInOneDynamicTaskGraph(DynamicTaskGraph):
    
    def __init__(self, task_queue):
        DynamicTaskGraph.__init__(self)
        self.task_queue = task_queue

    def task_runnable(self, task):
        self.task_queue.put(task)

class QueueMasterProxy:
    
    def __init__(self, response_queue):
        self.response_queue = response_queue
        
    def publish_refs(self, task_id, refs):
        self.response_queue.put((ACTION_PUBLISH, (task_id, refs)))
     
    def report_tasks(self, job_id, root_task_id, report):
        self.response_queue.put((ACTION_REPORT, (report, root_task_id)))
        
class AllInOneMasterProxy:
    
    def __init__(self, task_graph, task_runner):
        self.task_graph = task_graph
        self.task_runner = task_runner
    
    def publish_refs(self, task_id, refs):
        tx = TaskGraphUpdate()
        task = self.task_graph.get_task(task_id)
        for ref in refs:
            tx.publish(ref, task)
        tx.commit(self.task_graph)
     
    def spawn_tasks(self, parent_task_id, tasks):
        parent_task = self.task_graph.get_task(parent_task_id)
        
        tx = TaskGraphUpdate()
        
        for task_descriptor in tasks:
            task_object = build_taskpool_task_from_descriptor(task_descriptor, None, parent_task)
            tx.spawn(task_object)
        
        tx.commit(self.task_graph)
        
    def commit_task(self, task_id, bindings, saved_continuation_uri=None, replay_uuid_list=None):
        
        task = self.task_graph.get_task(task_id)
        tx = TaskGraphUpdate()
        
        for id, ref in bindings.items():
            tx.publish(ref, task)
            
        tx.commit(self.task_graph)

        task.state = TASK_COMMITTED
        
    def report_tasks(self, report, toplevel_task_id):

        task = self.task_graph.get_task(toplevel_task_id)

        tx = TaskGraphUpdate()
        
        for (parent_id, success, payload) in report:
            parent_task = self.task_graph.get_task(parent_id)
            if success:
                (spawned, published) = payload
                
                for child in spawned:
                    child_task = build_taskpool_task_from_descriptor(child, parent_task)
                    tx.spawn(child_task)
                    parent_task.children.append(child_task)
                
                for ref in published:
                    tx.publish(ref, parent_task)
            
            else:
                # Only one failed task per-report, at the moment.
                self.investigate_task_failure(parent_task, payload)
                self.lazy_task_pool.worker_pool.worker_idle(toplevel_task_id.worker)
                ciel.engine.publish('schedule')
                return
                
        tx.commit(self.task_graph)
        #self.task_graph.reduce_graph_for_references(toplevel_task.expected_outputs)
        
    def failed_task(self, task_id, reason=None, details=None, bindings={}):
        raise
        
class TaskRunner:
    
    def __init__(self, initial_task, initial_cont_ref, block_store, options):
        self.block_store = block_store
        self.task_queue = multiprocessing.Queue()
        self.response_queue = multiprocessing.Queue()
        self.task_graph = AllInOneDynamicTaskGraph(self.task_queue)
    
        self.master_proxy = AllInOneMasterProxy(self.task_graph, self)
        self.execution_features = ExecutionFeatures()

        self.initial_task = initial_task
        self.initial_cont_ref = initial_cont_ref
    
        self.job_output = AllInOneJobOutput()
        self.task_graph.subscribe(self.initial_task.expected_outputs[0], self.job_output)

        self.options = options

        self.is_running = False
        self.num_workers = options.num_threads
        self.workers = None

    def run(self):
        self.task_graph.publish(self.initial_cont_ref, None)
        self.task_graph.spawn(self.initial_task, None)

        self.is_running = True
        
        self.workers = []

        ciel.log('Starting %d worker threads.' % self.num_workers, 'TASKRUNNER', logging.INFO)        
        for _ in range(self.num_workers):
            try:
                self.workers.append(multiprocessing.Process(target=worker_process_main, args=(self.options.blockstore, self.task_queue, self.response_queue)))
            except:
                print sys.exc_info()

        response_handler_thread = threading.Thread(target=self.response_handler_thread_main)
        response_handler_thread.start()

        ciel.log('Starting %d worker threads.' % self.num_workers, 'TASKRUNNER', logging.INFO)        
        for worker in self.workers:
            worker.start()
        
        result = self.job_output.join()
        
        self.is_running = False
        for worker in self.workers:
            self.task_queue.put(THREAD_TERMINATOR)
        self.response_queue.put((ACTION_STOP, None))
        response_handler_thread.join()
        for worker in self.workers:
            worker.join()
            
        return result
    
    def response_handler_thread_main(self):
        
        while self.is_running:
            
            (action, args) = self.response_queue.get()
            
            if action == ACTION_SPAWN:
                self.master_proxy.spawn_tasks(*args)
            elif action == ACTION_PUBLISH:
                self.master_proxy.publish_refs(*args)
            elif action == ACTION_COMPLETE:
                self.master_proxy.commit_task(*args)
            elif action == ACTION_REPORT:
                self.master_proxy.report_tasks(*args)
            elif action == ACTION_STOP:
                return
  
class PseudoWorker:
    
    def __init__(self, block_store):
        self.block_store = block_store
    
def worker_process_main(base_dir, task_queue, response_queue):
    
    master_proxy = QueueMasterProxy(response_queue)
    execution_features = ExecutionFeatures()
    block_store = BlockStore(ciel.engine, 'localhost', 8000, base_dir, True)
    
    # XXX: Broken because we now need a pseudoworker in place of a block_store.
    thread_task_executor = TaskExecutorPlugin(ciel.engine, PseudoWorker(block_store), master_proxy, execution_features, 1)
   
    while True:
        
        task = task_queue.get()
        if isinstance(task, ThreadTerminator):
            return
        
        task_descriptor = task.as_descriptor(False)

        thread_task_executor.handle_input(task_descriptor)
    
