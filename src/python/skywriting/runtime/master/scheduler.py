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

'''
Created on 15 Apr 2010

@author: dgm36
'''
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty
from skywriting.runtime.master.task_pool import TASK_QUEUED

class Scheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, task_pool, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, 1, 'schedule')
        self.worker_pool = worker_pool
        self.task_pool = task_pool
        
    def handle_input(self, input):
        idle_workers = self.worker_pool.get_idle_workers()
        
        attempt_count = 0
        while len(idle_workers) > 0:
            retry_workers = []
            for worker in idle_workers:
                try:
                    task = worker.queues[attempt_count].get(block=False)
                    # Skip over tasks that have been aborted or otherwise scheduled.
                    while task.state != TASK_QUEUED:
                        task = worker.queues[attempt_count].get(block=False)
                    self.worker_pool.execute_task_on_worker_id(worker.id, task)
                except Empty:
                    # Try again on next round of attempts.
                    retry_workers.append(worker)
                except IndexError:
                    # No more queues for worker: now truly idle.
                    pass
            idle_workers = retry_workers
            attempt_count += 1
            