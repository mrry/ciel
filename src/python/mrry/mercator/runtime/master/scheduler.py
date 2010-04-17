'''
Created on 15 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty

class Scheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, task_pool, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, 1, 'schedule')
        self.worker_pool = worker_pool
        self.task_pool = task_pool
        
    def handle_input(self, input):
        print 'Running the scheduler!'
        idle_workers = self.worker_pool.get_idle_worker_ids()
        print idle_workers
        for worker_id in idle_workers:
            try:
                task = self.task_pool.runnable_queue.get(block=False)
                print task.task_id, '--->', worker_id
                self.worker_pool.execute_task_on_worker_id(worker_id, task)
            except Empty:
                return
            