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

from skywriting.runtime.plugins import AsynchronousExecutePlugin
import Queue

'''
Created on 15 Apr 2010

@author: dgm36
'''

class PushyScheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, 1, 'schedule')
        self.worker_pool = worker_pool
        self.scheduler_queue = Queue.Queue()
        
    def handle_input(self, input):
        
        # 1. Read runnable tasks from the scheduler's task queue, and assign
        #    them to workers.
        queue = self.scheduler_queue
        while True:
            try:
                task = queue.get_nowait()
            except Queue.Empty:
                break
            
            task.job.assign_scheduling_class_to_task(task)
            workers = task.job.assign_task_to_workers(task, self.worker_pool)
            
            for worker in workers:
                self.worker_pool.execute_task_on_worker(worker, task)
