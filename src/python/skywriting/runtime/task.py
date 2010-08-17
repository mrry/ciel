'''
Created on 17 Aug 2010

@author: dgm36
'''
import datetime
from skywriting.runtime.references import SW2_FutureReference
import uuid
import time

TASK_CREATED = -1
TASK_BLOCKING = 0
TASK_SELECTING = 1
TASK_RUNNABLE = 2
TASK_QUEUED = 3
TASK_ASSIGNED = 4
TASK_COMMITTED = 5
TASK_FAILED = 6
TASK_ABORTED = 7

TASK_STATES = {'CREATED': TASK_CREATED,
               'BLOCKING': TASK_BLOCKING,
               'SELECTING': TASK_SELECTING,
               'RUNNABLE': TASK_RUNNABLE,
               'QUEUED': TASK_QUEUED,
               'ASSIGNED': TASK_ASSIGNED,
               'COMMITTED': TASK_COMMITTED,
               'FAILED': TASK_FAILED,
               'ABORTED': TASK_ABORTED}

TASK_STATE_NAMES = {}
for (name, number) in TASK_STATES.items():
    TASK_STATE_NAMES[number] = name

class Task:

    def __init__(self, task_id, parent_task_id, handler, inputs, dependencies_are_processed, expected_outputs, save_continuation=False, continues_task=None, replay_uuids=None, select_group=None, select_result=None, state=TASK_CREATED):
        self.task_id = task_id
        
        # Task creation graph.
        self.parent = parent_task_id
        self.children = []
        self.continues_task = continues_task
        self.continuation = None
        
        self.original_task_id = None
        self.replay_ref = None
        
        self.handler = handler
        
        if dependencies_are_processed:
            self.inputs = inputs
            self.dependencies = inputs
        else:
            self.inputs = {}
            self.dependencies = inputs

        self.select_group = select_group
        self.select_result = select_result
            
        self.expected_outputs = expected_outputs
        
        self.save_continuation = save_continuation
        
        self.replay_uuids = replay_uuids

    def __repr__(self):
        return 'Task(%s)' % self.task_id

class TaskPoolTask(Task):
    
    def __init__(self, task_id, parent_task_id, handler, inputs, dependencies_are_processed, expected_outputs, save_continuation=False, continues_task=None, replay_uuids=None, select_group=None, select_result=None, state=TASK_CREATED, task_pool=None):
        Task.__init__(self, task_id, parent_task_id, handler, inputs, dependencies_are_processed, expected_outputs, save_continuation, continues_task, replay_uuids, select_group, select_result, state)
        
        self.task_pool = task_pool
        
        self.state = state
        
        self._blocking_dict = {}
        if select_group is not None:
            self._selecting_dict = {}
            
        self.history = []
        
        self.worker_id = None
        self.saved_continuation_uri = None
        
        self.event_index = 0
        self.current_attempt = 0

    def record_event(self, description):
        self.history.append((datetime.datetime.now(), description))

    def check_dependencies(self, global_name_directory):
        
        if self.select_group is None:

            self.inputs = {}

            for local_id, input in self.dependencies.items():
                if isinstance(input, SW2_FutureReference):
                    global_id = input.id
                    refs = global_name_directory.get_refs_for_id(global_id)
                    if len(refs) > 0:
                        self.inputs[local_id] = refs[0]
                    else:
                        try:
                            self._blocking_dict[global_id].add(local_id)
                        except KeyError:
                            self._blocking_dict[global_id] = set([local_id])
                else:
                    self.inputs[local_id] = input
    
            if len(self._blocking_dict) > 0:
                self.state = TASK_BLOCKING
            else:
                self.state = TASK_RUNNABLE
                self.record_event("RUNNABLE")
                
        else:
            
            # select()-handling code.
            if self.select_result is None:
                
                self.select_result = []
                for i, ref in enumerate(self.select_group):
                    if isinstance(ref, SW2_FutureReference):
                        global_id = ref.id
                        refs = global_name_directory.get_refs_for_id(global_id)
                        if len(refs) > 0:
                            self.select_result.append(i)
                        else:
                            self.selecting_dict[global_id] = i
                    else:
                        self.select_result.append(i)

                if len(self.select_group) > 0 and len(self.select_result) == 0:
                    self.state = TASK_SELECTING
                else:
                    self.state = TASK_RUNNABLE
                    self.record_event("RUNNABLE")
        
            else:
                
                # We are replaying a task that has previously returned from a call to select().
                # TODO: We need to make sure we handle blocking/failure for this task correctly.
                self.state = TASK_RUNNABLE
                self.record_event("RUNNABLE")
        
    def is_replay_task(self):
        return self.replay_ref is not None
        
    def is_blocked(self):
        return self.state in (TASK_BLOCKING, TASK_SELECTING)
        
    def blocked_on(self):
        if self.state == TASK_SELECTING:
            return self._selecting_dict.keys()
        elif self.state == TASK_BLOCKING:
            return self._blocking_dict.keys()
        else:
            return []

    def unblock_on(self, global_id, refs):
        if self.state in (TASK_RUNNABLE, TASK_SELECTING):
            i = self._selecting_dict.pop(global_id)
            self.select_result.append(i)
            self.state = TASK_RUNNABLE
            self.record_event("RUNNABLE")
        elif self.state in (TASK_BLOCKING,):
            local_ids = self._blocking_dict.pop(global_id)
            for local_id in local_ids:
                self.inputs[local_id] = refs[0]
            if len(self._blocking_dict) == 0:
                self.state = TASK_RUNNABLE
                self.record_event("RUNNABLE")
        
    # Warning: called under worker_pool._lock
    def set_assigned_to_worker(self, worker_id):
        self.worker_id = worker_id
        self.state = TASK_ASSIGNED
        self.record_event("ASSIGNED")
        self.task_pool.notify_task_assigned_to_worker_id(self, worker_id)

    def make_replay_task(self, replay_task_id, replay_ref):
        
        ret = TaskPoolTask(replay_task_id, self.parent, self.handler, self.inputs, True, self.expected_outputs, self.save_continuation, self.continues_task, self.replay_uuids, self.select_group, self.select_result, TASK_RUNNABLE, self.task_pool)
        ret.original_task_id = self.task_id
        ret.replay_ref = replay_ref
        return ret

    def as_descriptor(self, long=False):        
        descriptor = {'task_id': str(self.task_id),
                      'dependencies': self.dependencies,
                      'handler': self.handler,
                      'expected_outputs': map(str, self.expected_outputs),
                      'inputs': self.inputs,
                      'event_index': self.event_index}
        
        if long:
            descriptor['history'] = map(lambda (t, name): (time.mktime(t.timetuple()) + t.microsecond / 1e6, name), self.history)
            descriptor['worker_id'] = self.worker_id
            descriptor['saved_continuation_uri'] = self.saved_continuation_uri
            descriptor['state'] = TASK_STATE_NAMES[self.state]
            descriptor['parent'] = str(self.parent)
            descriptor['children'] = map(str, self.children)
                    
        if self.select_result is not None:
            descriptor['select_result'] = self.select_result
        
        if self.save_continuation:
            descriptor['save_continuation'] = True
        if self.continues_task is not None:
            descriptor['continues_task'] = str(self.continues_task)
        if self.continuation is not None:
            descriptor['continuation'] = str(self.continuation)
        if self.replay_uuids is not None:
            descriptor['replay_uuids'] = map(str, self.replay_uuids)
            
        if self.original_task_id is not None:
            descriptor['original_task_id'] = str(self.original_task_id)
        if self.replay_ref is not None:
            descriptor['replay_ref'] = self.replay_ref
        
        return descriptor        


def build_taskpool_task_from_descriptor(task_id, task_descriptor, global_name_directory, task_pool, parent_task_id=None):

    handler = task_descriptor['handler']
    inputs = task_descriptor['inputs']
    dependencies_are_processed = False
    expected_outputs = task_descriptor['expected_outputs']

    try:
        save_continuation = task_descriptor['save_continuation']
    except KeyError:
        save_continuation = False

    try:
        continues_task = uuid.UUID(hex=task_descriptor['continues_task'])
    except KeyError:
        continues_task = None

    try:
        select_group = task_descriptor['select_group']
    except:
        select_group = None
    select_result = None

    replay_uuids = None
    
    state = TASK_CREATED
    
    return TaskPoolTask(task_id, parent_task_id, handler, inputs, dependencies_are_processed, expected_outputs, save_continuation, continues_task, replay_uuids, select_group, select_result, state, task_pool)
#
#class Task:
#    
#    def __init__(self, task_id, task_descriptor, global_name_directory, task_pool, parent_task_id=None):
#        self.task_id = task_id
#        self.handler = task_descriptor['handler']
#        self.inputs = {}
#        self.current_attempt = 0
#        self.worker_id = None
#        self.event_index = 0
#        self.task_pool = task_pool
#        
#        self.history = []
#       
#        self.parent = parent_task_id
#        self.children = []
#        
#        self.dependencies = task_descriptor['inputs']
#        
#        self.inputs = {}
#                
#        self.expected_outputs = task_descriptor['expected_outputs']
#    
#        try:
#            self.save_continuation = task_descriptor['save_continuation']
#        except KeyError:
#            self.save_continuation = False
#
#        try:
#            self.continues_task = uuid.UUID(hex=task_descriptor['continues_task'])
#        except KeyError:
#            self.continues_task = None
#
#        self.replay_uuids = None
#        self.saved_continuation_uri = None
#    
#        self.state = TASK_RUNNABLE    
#    
#        self.check_dependencies(global_name_directory)
#            
#        # select()-handling code.
#        try:
#            self.select_group = task_descriptor['select_group']
#        except:
#            pass
#            
#            self.selecting_dict = {}
#            self.select_result = []
#            
#            for i, ref in enumerate(self.select_group):
#                if isinstance(ref, SW2_FutureReference):
#                    global_id = ref.id
#                    refs = global_name_directory.get_refs_for_id(global_id)
#                    if len(refs) > 0:
#                        self.select_result.append(i)
#                    else:
#                        self.selecting_dict[global_id] = i
#                else:
#                    self.select_result.append(i)
#
#            if len(self.select_group) > 0 and len(self.select_result) == 0:
#                self.state = TASK_SELECTING
#            
#        except KeyError:
#            self.select_group = None
#        
#        self.record_event("CREATED")
#
#    # Warning: called under worker_pool._lock
#    def set_assigned_to_worker(self, worker_id):
#        self.worker_id = worker_id
#        self.state = TASK_ASSIGNED
#        self.record_event("ASSIGNED")
#        self.task_pool.notify_task_assigned_to_worker_id(self, worker_id)
#        
#    def __repr__(self):
#        return 'Task(%d)' % self.task_id
#       
#    def record_event(self, description):
#        self.history.append((datetime.datetime.now(), description))
#        
#    def check_dependencies(self, global_name_directory):
#        self.blocking_dict = {}
#        for local_id, input in self.dependencies.items():
#            if isinstance(input, SW2_FutureReference):
#                global_id = input.id
#                refs = global_name_directory.get_refs_for_id(global_id)
#                if len(refs) > 0:
#                    self.inputs[local_id] = refs[0]
#                else:
#                    try:
#                        self.blocking_dict[global_id].add(local_id)
#                    except KeyError:
#                        self.blocking_dict[global_id] = set([local_id])
#            else:
#                self.inputs[local_id] = input
#
#        if len(self.blocking_dict) > 0:
#            self.state = TASK_BLOCKING
#        
#    def is_blocked(self):
#        return self.state in (TASK_BLOCKING, TASK_SELECTING)
#            
#    def blocked_on(self):
#        if self.state == TASK_SELECTING:
#            return self.selecting_dict.keys()
#        elif self.state == TASK_BLOCKING:
#            return self.blocking_dict.keys()
#        else:
#            return []
#    
#    def unblock_on(self, global_id, refs):
#        if self.state in (TASK_RUNNABLE, TASK_SELECTING):
#            i = self.selecting_dict.pop(global_id)
#            self.select_result.append(i)
#            self.state = TASK_RUNNABLE
#            self.record_event("RUNNABLE")
#        elif self.state in (TASK_BLOCKING,):
#            local_ids = self.blocking_dict.pop(global_id)
#            for local_id in local_ids:
#                self.inputs[local_id] = refs[0]
#            if len(self.blocking_dict) == 0:
#                self.state = TASK_RUNNABLE
#                self.record_event("RUNNABLE")
#        
#    def as_descriptor(self, long=False):        
#        descriptor = {'task_id': str(self.task_id),
#                      'dependencies': self.dependencies,
#                      'handler': self.handler,
#                      'expected_outputs': map(str, self.expected_outputs),
#                      'inputs': self.inputs,
#                      'event_index': self.event_index}
#        
#        if long:
#            descriptor['history'] = map(lambda (t, name): (time.mktime(t.timetuple()) + t.microsecond / 1e6, name), self.history)
#            descriptor['worker_id'] = self.worker_id
#            descriptor['saved_continuation_uri'] = self.saved_continuation_uri
#            descriptor['state'] = TASK_STATE_NAMES[self.state]
#            descriptor['parent'] = str(self.parent)
#            descriptor['children'] = map(str, self.children)
#                    
#        if hasattr(self, 'select_result'):
#            descriptor['select_result'] = self.select_result
#        
#        if self.save_continuation:
#            descriptor['save_continuation'] = True
#        if self.continues_task:
#            descriptor['continues_task'] = str(self.continues_task)
#        if self.replay_uuids is not None:
#            descriptor['replay_uuids'] = map(str, self.replay_uuids)
#        
#        return descriptor        
