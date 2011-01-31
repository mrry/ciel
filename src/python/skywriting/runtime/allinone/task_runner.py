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
import Queue
from skywriting.runtime.task_executor import TaskExecutorPlugin
from skywriting.runtime.executors import ExecutionFeatures
import cherrypy
from skywriting.runtime.task import build_taskpool_task_from_descriptor,\
    TASK_COMMITTED
import ciel

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class AllInOneJobOutput:
    
    def __init__(self):
        self.event = threading.Event()
        self.result = None
   
    def is_queued_streaming(self):
        return False
    def is_assigned_streaming(self):
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
            task_object = build_taskpool_task_from_descriptor(task_descriptor['task_id'], task_descriptor, None, parent_task)
            tx.spawn(task_object)
        
        tx.commit(self.task_graph)
        
    def commit_task(self, task_id, bindings, saved_continuation_uri=None, replay_uuid_list=None):
        
        task = self.task_graph.get_task(task_id)
        tx = TaskGraphUpdate()
        
        for id, ref in bindings.items():
            tx.publish(ref, task)
            
        tx.commit(self.task_graph)

        task.state = TASK_COMMITTED
        
    def failed_task(self, task_id, reason=None, details=None, bindings={}):
        raise
        
class TaskRunner:
    
    def __init__(self, initial_task, initial_cont_ref, block_store, num_workers=1):
        self.block_store = block_store
        self.task_queue = Queue.Queue()
        self.task_graph = AllInOneDynamicTaskGraph(self.task_queue)
    
        self.master_proxy = AllInOneMasterProxy(self.task_graph, self)
        self.execution_features = ExecutionFeatures()

        self.initial_task = initial_task
        self.initial_cont_ref = initial_cont_ref
    
        self.job_output = AllInOneJobOutput()
        self.task_graph.subscribe(self.initial_task.expected_outputs[0], self.job_output)

        self.is_running = False
        self.num_workers = num_workers
        self.workers = None

    def run(self):
        self.task_graph.publish(self.initial_cont_ref, None)
        self.task_graph.spawn(self.initial_task, None)

        self.is_running = True
        
        self.workers = []
        for _ in range(self.num_workers):
            self.workers.append(threading.Thread(target=self.worker_thread_main))
        
        for worker in self.workers:
            worker.start()
        
        result = self.job_output.join()
        
        self.is_running = False
        for worker in self.workers:
            self.task_queue.put(THREAD_TERMINATOR)
        for worker in self.workers:
            worker.join()
            
        return result
    
    def worker_thread_main(self):
    
        # FIXME: Set skypybase appropriately.
        thread_task_executor = TaskExecutorPlugin(ciel.engine, None, self.block_store, self.master_proxy, self.execution_features, 1)
    
        while self.is_running:
            
            task = self.task_queue.get()
            if task is THREAD_TERMINATOR:
                return
            
            task_descriptor = task.as_descriptor(False)
    
            thread_task_executor.handle_input(task_descriptor)
    
    