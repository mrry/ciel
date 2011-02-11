from skywriting.runtime.task_graph import DynamicTaskGraph, TaskGraphUpdate
from skywriting.runtime.task import build_taskpool_task_from_descriptor

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

    def __init__(self, root_task_id, execution_features):
        DynamicTaskGraph.__init__(self)
        self.root_task_id = root_task_id
        self.execution_features = execution_features
        self.runnable_small_tasks = []

    def spawn_and_publish(self, spawns, refs, producer=None):
        
        producer_task = None
        if producer is not None:
            producer_task = self.get_task(producer["task_id"])
        upd = TaskGraphUpdate()
        for spawn in spawns:
            task_object = build_taskpool_task_from_descriptor(spawn['task_id'], spawn, None, producer_task)
            upd.spawn(task_object)
        for ref in refs:
            upd.publish(ref, producer_task)
        upd.commit(self)

    def task_runnable(self, task):
        td = task.as_descriptor()
        if self.execution_features.can_run(td["handler"]):
            if td["task_id"] == self.root_task_id or ("hint" in td["task_private"] and td["task_private"]["hint"] == "small_task"):
                self.runnable_small_tasks.append(td)

    def get_runnable_task(self):
        return self.runnable_small_tasks.pop()
