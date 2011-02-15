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
from skywriting.runtime.references import SWURLReference, SW2_ConcreteReference, SW2_SweetheartReference
from skywriting.runtime.block_store import get_netloc_for_sw_url
from skywriting.runtime.task import TASK_QUEUED, TASK_QUEUED_STREAMING
import cherrypy
import logging
import random

'''
Created on 15 Apr 2010

@author: dgm36
'''
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty

SWEETHEART_FACTOR = 1000
EQUALLY_LOCAL_MARGIN = 0.9

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

        # XXX: Shuffle the idle workers to prevent all tasks ending up on the same worker (when we have an idle cluster).
        random.shuffle(idle_workers)
        attempt_count = 0
        while len(idle_workers) > 0:
            retry_workers = []
            for worker in idle_workers:
                try:
                    task = worker.queues[attempt_count].get(block=False)
                    # Skip over tasks that have been aborted or otherwise scheduled.
                    while task.state != TASK_QUEUED and task.state != TASK_QUEUED_STREAMING:
                        task = worker.queues[attempt_count].get(block=False)
                    self.worker_pool.execute_task_on_worker(worker, task)
                except Empty:
                    # Try again on next round of attempts.
                    retry_workers.append(worker)
                except IndexError:
                    # No more queues for worker: now truly idle.
                    pass
            idle_workers = retry_workers
            attempt_count += 1

    # Based on TaskPool.compute_best_worker_for_task()
    def compute_good_workers_for_task(self, task):
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
            elif isinstance(input, SW2_SweetheartReference) and input.size_hint is not None:
                try:
                    current_saving_for_netloc = netlocs[input.sweetheart_netloc]
                except KeyError:
                    current_saving_for_netloc = 0
                netlocs[netloc] = current_saving_for_netloc + SWEETHEART_FACTOR * input.size_hint
                
                # Accord the unboosted saving to other locations.
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
            elif isinstance(input, SW2_ConcreteReference) and input.size_hint is not None:
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
        ranked_netlocs = [(saving, netloc) for (netloc, saving) in netlocs.items()]
        filtered_ranked_netlocs = filter(lambda (saving, netloc) : self.worker_pool.get_worker_at_netloc(netloc) is not None, ranked_netlocs)
        if len(filtered_ranked_netlocs) > 0:
            max_saving = max(filtered_ranked_netlocs)[0]
            for saving, netloc in filtered_ranked_netlocs:
                if saving > (EQUALLY_LOCAL_MARGIN * max_saving):
                    yield self.worker_pool.get_worker_at_netloc(netloc) 
            
    # Based on TaskPool.add_task_to_queues()
    def add_task_to_worker_queues(self, task):
        if task.state == TASK_QUEUED_STREAMING:
            handler_queue = self.worker_pool.feature_queues.get_streaming_queue_for_feature(task.handler)
            handler_queue.put(task)
        elif task.state == TASK_QUEUED:
            handler_queue = self.worker_pool.feature_queues.get_queue_for_feature(task.handler)
            handler_queue.put(task)
            for good_worker in self.compute_good_workers_for_task(task):
                good_worker.local_queue.put(task)
        else:
            cherrypy.log.error("Task %s scheduled in bad state %s; ignored" % (task, task.state), 
                               "SCHEDULER", logging.ERROR)
