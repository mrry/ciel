'''
Created on 15 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty
from mrry.mercator.runtime.master.task_pool import TASK_QUEUED

class Scheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, task_pool, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, 1, 'schedule')
        self.worker_pool = worker_pool
        self.task_pool = task_pool
        
    def handle_input(self, input):
        print 'Running the scheduler!'
        idle_workers = self.worker_pool.get_idle_workers()
        print idle_workers
        
        attempt_count = 0
        while len(idle_workers) > 0:
            retry_workers = []
            for worker in idle_workers:
                try:
                    print "Trying to find a task for worker", worker.id, "in queue:", worker.queues[attempt_count]
                    task = worker.queues[attempt_count].get(block=False)
                    # Skip over tasks that have been aborted or otherwise scheduled.
                    while task.state != TASK_QUEUED:
                        task = worker.queues[attempt_count].get(block=False)
                    print '%d -%d-> %d' % (task.task_id, attempt_count, worker.id)
                    self.worker_pool.execute_task_on_worker_id(worker.id, task)
                except Empty:
                    # Try again on next round of attempts.
                    retry_workers.append(worker)
                except IndexError:
                    # No more queues for worker: now truly idle.
                    print "No work for worker %d" % worker.id
            idle_workers = retry_workers
            attempt_count += 1
            print len(idle_workers)
            