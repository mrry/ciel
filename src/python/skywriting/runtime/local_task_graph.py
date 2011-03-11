# Copyright (c) 2011 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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
from skywriting.runtime.task import build_taskpool_task_from_descriptor
import Queue
import ciel
import logging

class LocalJobOutput:
    
    def __init__(self, required_refs, taskset=None):
        self.required_refs = set(required_refs)
        self.taskset = taskset
    def is_queued_streaming(self):
        return False
    def is_blocked(self):
        return True
    def is_complete(self):
        return len(self.required_refs) == 0
    def notify_ref_table_updated(self, ref_table_entry):
        self.required_refs.remove(ref_table_entry.ref)
        # Commented out because the refcounting should take care of it.
        #if self.is_complete() and self.taskset is not None:
        #    self.taskset.notify_completed()

class LocalTaskGraph(DynamicTaskGraph):

    def __init__(self, execution_features, runnable_queues):
        DynamicTaskGraph.__init__(self)
        self.root_task_ids = set()
        self.execution_features = execution_features
        self.runnable_queues = runnable_queues

    def add_root_task_id(self, root_task_id):
        self.root_task_ids.add(root_task_id)
        
    def remove_root_task_id(self, root_task_id):
        self.root_task_ids.remove(root_task_id)

    def spawn_and_publish(self, spawns, refs, producer=None, taskset=None):
        
        producer_task = None
        if producer is not None:
            producer_task = self.get_task(producer["task_id"])
            taskset = producer_task.taskset
        upd = TaskGraphUpdate()
        for spawn in spawns:
            task_object = build_taskpool_task_from_descriptor(spawn, producer_task, taskset)
            upd.spawn(task_object)
        for ref in refs:
            upd.publish(ref, producer_task)
        upd.commit(self)

    def task_runnable(self, task):
        ciel.log('Task %s became runnable!' % task.task_id, 'LTG', logging.INFO)
        if self.execution_features.can_run(task.handler):
            if task.task_id in self.root_task_ids:
                ciel.log('Putting task %s in the runnableQ because it is a root' % task.task_id, 'LTG', logging.INFO)
                try:
                    self.runnable_queues[task.scheduling_class].put(task)
                except KeyError:
                    try:
                        self.runnable_queues['*'].put(task)
                    except KeyError:
                        ciel.log('Scheduling class %s not supported on this worker (for task %s)' % (task.scheduling_class, task.task_id), 'LTG', logging.ERROR)
                        raise
                task.taskset.inc_runnable_count()
            else:
                try:
                    is_small_task = task.worker_private['hint'] == 'small_task'
                    if is_small_task:
                        ciel.log('Putting task %s in the runnableQ because it is small' % task.task_id, 'LTG', logging.INFO)
                        try:
                            self.runnable_queues[task.scheduling_class].put(task)
                        except KeyError:
                            try:
                                self.runnable_queues['*'].put(task)
                            except KeyError:
                                ciel.log('Scheduling class %s not supported on this worker (for task %s)' % (task.scheduling_class, task.task_id), 'LTG', logging.ERROR)
                                raise
                        self.taskset.inc_runnable_count()
                except KeyError:
                    pass
                except AttributeError:
                    pass

    def get_runnable_task(self):
        ret = self.get_runnable_task_as_task()
        if ret is not None:
            ret = ret.as_descriptor()
        return ret
        
    def get_runnable_task_as_task(self):
        try:
            return self.runnable_small_tasks.get_nowait()
        except Queue.Empty:
            return None
