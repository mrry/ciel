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
from shared.references import SW2_ConcreteReference, SW2_SweetheartReference,\
    SW2_StreamReference
from skywriting.runtime.task import TASK_QUEUED, TASK_QUEUED_STREAMING
import logging
import ciel
import random
import Queue

'''
Created on 15 Apr 2010

@author: dgm36
'''
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from Queue import Empty

MIN_SAVING_THRESHOLD = 1048576
STREAM_SOURCE_BYTES_EQUIVALENT = 10000000
SWEETHEART_FACTOR = 1000
EQUALLY_LOCAL_MARGIN = 0.9

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
            except Empty:
                break
            
            workers = self.assign_task_to_workers(task)
            
            for worker in workers:
                self.worker_pool.execute_task_on_worker(worker, task)
        
        
        # 2. Assign workers tasks from their respective queues.
        workers = self.worker_pool.get_all_workers()# get_idle_workers()

    # Based on TaskPool.compute_best_worker_for_task()
    def compute_best_location_for_task(self, task):
        netlocs = {}

        for input in task.inputs.values():
            
            if isinstance(input, SW2_SweetheartReference) and input.size_hint is not None:
                # Sweetheart references get a boosted benefit for the sweetheart, and unboosted benefit for all other netlocs.
                try:
                    current_saving_for_netloc = netlocs[input.sweetheart_netloc]
                except KeyError:
                    current_saving_for_netloc = 0
                netlocs[input.sweetheart_netloc] = current_saving_for_netloc + SWEETHEART_FACTOR * input.size_hint
                
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
                    
            elif isinstance(input, SW2_ConcreteReference) and input.size_hint is not None:
                # Concrete references get an unboosted benefit for all netlocs.
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
                    
            elif isinstance(input, SW2_StreamReference):
                # Stream references get a heuristically-chosen benefit for stream sources.
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + STREAM_SOURCE_BYTES_EQUIVALENT
                    
        ranked_netlocs = [(saving, netloc) for (netloc, saving) in netlocs.items()]
        filtered_ranked_netlocs = filter(lambda (saving, netloc) : self.worker_pool.get_worker_at_netloc(netloc) is not None and saving > MIN_SAVING_THRESHOLD, ranked_netlocs)
        if len(filtered_ranked_netlocs) > 0:
            return self.worker_pool.get_worker_at_netloc(max(filtered_ranked_netlocs)[1])
        else:
            # If we have no preference for any worker, use the power of two random choices. [Azar et al. STOC 1994]
            worker1 = self.worker_pool.get_random_worker()
            worker2 = self.worker_pool.get_random_worker()
            if worker1.load() < worker2.load():
                return worker1
            else:
                return worker2
            
    def assign_task_to_workers(self, task):
        if task.has_constrained_location():
            fixed_netloc = task.get_constrained_location()
            task.assign_netloc(fixed_netloc)
            return [self.worker_pool.get_worker_at_netloc(fixed_netloc)]
        elif task.state in (TASK_QUEUED_STREAMING, TASK_QUEUED):
            best_worker = self.compute_best_location_for_task(task)
            task.assign_netloc(best_worker.netloc)
            return [best_worker]
        else:
            ciel.log.error("Task %s scheduled in bad state %s; ignored" % (task, task.state), 
                               "SCHEDULER", logging.ERROR)
            return []      
