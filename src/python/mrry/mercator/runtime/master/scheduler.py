'''
Created on 15 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty

class Scheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, num_threads, task_pool, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, 'schedule')
        self.worker_pool = worker_pool
        self.task_pool = task_pool
        
    def handle_input(self, input):
        idle_workers = self.worker_pool.get_idle_workers()
        
        for worker in idle_workers:
            try:
                task = self.task_pool.runnable_queue.get()
                worker.execute_task_on_worker(worker, task)
            except Empty:
                return
            