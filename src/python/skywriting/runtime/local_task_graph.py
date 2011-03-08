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

class LocalJobOutput:
    
    def __init__(self, required_refs):
        self.required_refs = set(required_refs)
    def is_queued_streaming(self):
        return False
    def is_assigned_streaming(self):
        return False
    def is_blocked(self):
        return True
    def is_complete(self):
        return len(self.required_refs) == 0
    def notify_ref_table_updated(self, ref_table_entry):
        self.required_refs.remove(ref_table_entry.ref)

class LocalTaskGraph(DynamicTaskGraph):

    def __init__(self, execution_features, root_task_ids=[]):
        DynamicTaskGraph.__init__(self)
        self.root_task_ids = set(root_task_ids)
        self.execution_features = execution_features
        self.runnable_small_tasks = Queue.Queue()

    def add_root_task_id(self, root_task_id):
        self.root_task_ids.add(root_task_id)
        
    def remove_root_task_id(self, root_task_id):
        self.root_task_ids.remove(root_task_id)

    def spawn_and_publish(self, spawns, refs, producer=None):
        
        producer_task = None
        if producer is not None:
            producer_task = self.get_task(producer["task_id"])
        upd = TaskGraphUpdate()
        for spawn in spawns:
            task_object = build_taskpool_task_from_descriptor(spawn, producer_task)
            upd.spawn(task_object)
        for ref in refs:
            upd.publish(ref, producer_task)
        upd.commit(self)

    def task_runnable(self, task):
        td = task.as_descriptor()
        if self.execution_features.can_run(td["handler"]):
            if td["task_id"] in self.root_task_ids:
                self.runnable_small_tasks.put(td)
            else:
                try:
                    is_small_task = td["worker_private"]["hint"] == "small_task"
                    if is_small_task:
                        self.runnable_small_tasks.put(td)
                except KeyError:
                    pass

    def get_runnable_task(self):
        try:
            return self.runnable_small_tasks.get_nowait()
        except Queue.Empty:
            return None
