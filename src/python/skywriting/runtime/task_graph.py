# Copyright (c) 2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from shared.references import SW2_FutureReference, combine_references,\
    SW2_StreamReference
from skywriting.runtime.task import TASK_CREATED, TASK_BLOCKING, TASK_COMMITTED,\
    TASK_RUNNABLE
import collections

class ReferenceTableEntry:
    """Represents information stored about a reference in the task graph."""
    
    def __init__(self, ref, producing_task=None): 
        self.ref = ref
        self.producing_task = producing_task
        self.consumers = None

    def update_producing_task(self, task):
        self.producing_task = task

    def combine_references(self, other_ref):
        self.ref = combine_references(self.ref, other_ref)

    def has_consumers(self):
        return self.consumers is not None and len(self.consumers) > 0

    def add_consumer(self, task):
        """
        task must implement the notify_ref_table_updated(self, ref_table_entry)
        method.
        """
        if self.consumers is None:
            self.consumers = set([task])
        else:
            self.consumers.add(task)

    def remove_consumer(self, task):
        if self.consumers is not None:
            self.consumers.remove(task)

class TaskGraphUpdate:
    
    def __init__(self):
        self.spawns = []
        self.publishes = []
        self.reduce_list = []

    def spawn(self, task):
        self.spawns.append(task)
        
    def publish(self, reference, producing_task=None):
        self.publishes.append((reference, producing_task))

    def commit(self, graph):
        for (reference, producing_task) in self.publishes:
            graph.publish(reference, producing_task)
            
        for task in self.spawns:
            graph.spawn(task, self)
            
        graph.reduce_graph_for_tasks(self.reduce_list)

class DynamicTaskGraph:
    
    def __init__(self):
        
        # Mapping from task ID to task object.
        self.tasks = {}
        
        # Mapping from reference ID to reference table entry.
        self.references = {}
        
    def spawn(self, task, tx=None):
        """Add a new task to the graph. If tx is None, this will cause an immediate
        reduction; otherwise, tasks-to-reduce will be added to tx.result_list."""
        
        # Record the task in the task table.
        self.tasks[task.task_id] = task
        
        # Now update the reference table to account for the new task.
        # We will need to reduce this task if any of its outputs have consumers. 
        should_reduce = False
        for output_id in task.expected_outputs:
            ref_table_entry = self.publish(SW2_FutureReference(output_id), task)
            should_reduce = should_reduce or ref_table_entry.has_consumers()
            
        if should_reduce:
            if tx is not None:
                tx.reduce_list.append(task)
            else:
                self.reduce_graph_for_tasks([task])
    
    def publish(self, reference, producing_task=None):
        """Updates the information held about a reference. Returns the updated
        reference table entry for the reference."""
        try:
            
            ref_table_entry = self.get_reference_info(reference.id)
            if producing_task is not None:
                ref_table_entry.update_producing_task(producing_task)
            ref_table_entry.combine_references(reference)
            
            if ref_table_entry.has_consumers():
                consumers_copy = ref_table_entry.consumers.copy()
                for task in consumers_copy:
                    self.notify_task_of_reference(task, ref_table_entry)
                
        except KeyError:
            ref_table_entry = ReferenceTableEntry(reference, producing_task)
            self.references[reference.id] = ref_table_entry
        return ref_table_entry
    
    def subscribe(self, id, consumer):
        """
        Adds a consumer for the given ID. Typically, this is used to monitor
        job completion (by adding a synthetic task).
        """
        try:
            ref_table_entry = self.get_reference_info(id)
            if ref_table_entry.ref.is_consumable():
                consumer.notify_ref_table_updated(ref_table_entry)
        except KeyError:
            reference = SW2_FutureReference(id)
            ref_table_entry = ReferenceTableEntry(reference, None)
            self.references[reference.id] = ref_table_entry
            
        ref_table_entry.add_consumer(consumer)
            
    
    
    def notify_task_of_reference(self, task, ref_table_entry):
        if ref_table_entry.ref.is_consumable():
            was_queued_streaming = task.is_queued_streaming()
            was_blocked = task.is_blocked()
            task.notify_ref_table_updated(ref_table_entry)
            if was_blocked and not task.is_blocked():
                self.task_runnable(task)
            elif was_queued_streaming and not task.is_queued_streaming():
                # Submit this to the scheduler again
                self.task_runnable(task)
    
    def reduce_graph_for_references(self, ref_ids):
    
        root_tasks = []
    
        # Initially, start with the root set of tasks, based on the desired
        # object IDs.
        for ref_id in ref_ids:
            task = self.get_reference_info(ref_id).producing_task
            if task.state == TASK_CREATED:
                # Task has not yet been scheduled, so add it to the queue.
                task.set_state(TASK_BLOCKING)
                root_tasks.append(task)

        self.reduce_graph_for_tasks(root_tasks)
    
    def reduce_graph_for_tasks(self, root_tasks):
        
        newly_active_task_queue = collections.deque()
            
        for task in root_tasks:
            newly_active_task_queue.append(task)
                
        # Do breadth-first search through the task graph to identify other 
        # tasks to make active. We use task.state == TASK_BLOCKING as a marker
        # to prevent visiting a task more than once.
        while len(newly_active_task_queue) > 0:
            
            task = newly_active_task_queue.popleft()
            
            # Identify the other tasks that need to run to make this task
            # runnable.
            task_will_block = False
            for local_id, ref in task.dependencies.items():

                try:
                    ref_table_entry = self.get_reference_info(ref.id)
                    ref_table_entry.combine_references(ref)
                except KeyError:
                    ref_table_entry = ReferenceTableEntry(ref, None)
                    self.references[ref.id] = ref_table_entry

                if ref_table_entry.ref.is_consumable():
                    conc_ref = ref_table_entry.ref
                    task.inputs[local_id] = conc_ref
                    if isinstance(conc_ref, SW2_StreamReference):
                        task.unfinished_input_streams.add(ref.id)
                        ref_table_entry.add_consumer(task)

                else:
                    
                    # The reference is a future that has not yet been produced,
                    # so subscribe to the reference and block the task.
                    ref_table_entry.add_consumer(task)
                    task_will_block = True
                    task.block_on(ref.id, local_id)
                    
                    # We may need to recursively check the inputs on the
                    # producing task for this reference.
                    producing_task = ref_table_entry.producing_task
                    if producing_task is not None:
                        # The producing task is inactive, so recursively visit it.                    
                        if producing_task.state in (TASK_CREATED, TASK_COMMITTED):
                            producing_task.set_state(TASK_BLOCKING)
                            newly_active_task_queue.append(producing_task)
            
            # If all inputs are available, we can now run this task. Otherwise,
            # it will run when its inputs are published.
            if not task_will_block:
                task.set_state(TASK_RUNNABLE)
                self.task_runnable(task)
    
    def task_runnable(self, task):
        """
        Called when a task becomes runnable. Subclasses should provide their
        own implementation of this function.
        """
        raise NotImplementedError()
    
    def get_task(self, task_id):
        return self.tasks[task_id]
    
    def get_reference_info(self, ref_id):
        return self.references[ref_id]
