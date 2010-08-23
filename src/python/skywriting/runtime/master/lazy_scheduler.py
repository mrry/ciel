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
from skywriting.runtime.references import SWURLReference, SW2_ConcreteReference
from skywriting.runtime.block_store import get_netloc_for_sw_url

'''
Created on 15 Apr 2010

@author: dgm36
'''
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty
from skywriting.runtime.master.task_pool import TASK_QUEUED

class LazyScheduler(AsynchronousExecutePlugin):
    
    def __init__(self, bus, task_pool, worker_pool):
        AsynchronousExecutePlugin.__init__(self, bus, 1, 'schedule')
        self.worker_pool = worker_pool
        self.task_pool = task_pool
        
    def handle_input(self, input):
        
        # 1. Read runnable tasks from the task pool's task queue, and assign
        #    them to workers.
        queue = self.task_pool.get_task_queue()
        while True:
            try:
                task = queue.get_nowait()
                self.add_task_to_worker_queues(task)
            except Empty:
                break
        
        # 2. Assign workers tasks from their respective queues.
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

    # Based on TaskPool.compute_best_worker_for_task()
    def compute_best_worker_for_task(self, task):
        netlocs = {}
        for input in task.inputs.values():
            if isinstance(input, SWURLReference):
                if input.size_hint is None:
                    # XXX: We don't know the size of objects from outside the
                    # cluster. So we make a guess
                    # TODO: Do something sensible here; probably HTTP HEAD
                    input.size_hint = 10000000
                for url in input.urls:
                    netloc = get_netloc_for_sw_url(url)
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
            elif isinstance(input, SW2_ConcreteReference) and input.size_hint is not None:
                for netloc in input.location_hints.keys():
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
        ranked_netlocs = [(saving, netloc) for (netloc, saving) in netlocs.items()]
        filtered_ranked_netlocs = filter(lambda (saving, netloc) : self.worker_pool.get_worker_at_netloc(netloc) is not None, ranked_netlocs)
        if len(filtered_ranked_netlocs) > 0:
            ret = self.worker_pool.get_worker_at_netloc(max(filtered_ranked_netlocs)[1])
            return ret
        else:
            return None
    
    # Based on TaskPool.add_task_to_queues()
    def add_task_to_worker_queues(self, task):
        best_worker = self.compute_best_worker_for_task(task)
        if best_worker is not None:
            best_worker.local_queue.put(task)
        handler_queue = self.worker_pool.feature_queues.get_queue_for_feature(task.handler)
        handler_queue.put(task)