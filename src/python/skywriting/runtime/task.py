'''
Created on 17 Aug 2010

@author: dgm36
'''
import datetime
from skywriting.runtime.references import SW2_FutureReference, SW2_StreamReference
import time
import cherrypy
import logging

TASK_CREATED = -1
TASK_BLOCKING = 0
TASK_SELECTING = 1
TASK_RUNNABLE = 2
TASK_QUEUED_STREAMING = 3
TASK_QUEUED = 4
TASK_ASSIGNED_STREAMING = 5
TASK_ASSIGNED = 6
TASK_COMMITTED = 7
TASK_FAILED = 8
TASK_ABORTED = 9

TASK_STATES = {'CREATED': TASK_CREATED,
               'BLOCKING': TASK_BLOCKING,
               'SELECTING': TASK_SELECTING,
               'RUNNABLE': TASK_RUNNABLE,
               'QUEUED_STREAMING': TASK_QUEUED_STREAMING,
               'QUEUED': TASK_QUEUED,
               'ASSIGNED_STREAMING': TASK_ASSIGNED_STREAMING,
               'ASSIGNED': TASK_ASSIGNED,
               'COMMITTED': TASK_COMMITTED,
               'FAILED': TASK_FAILED,
               'ABORTED': TASK_ABORTED}

TASK_STATE_NAMES = {}
for (name, number) in TASK_STATES.items():
    TASK_STATE_NAMES[number] = name

class Task:

    def __init__(self, task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation=False, continues_task=None, replay_uuids=None, select_group=None, select_result=None, state=TASK_CREATED):
        self.task_id = task_id
        
        # Task creation graph.
        self.parent = parent_task
        self.children = []
        self.continues_task = continues_task
        self.continuation = None
        
        self.original_task_id = None
        self.replay_ref = None
        
        self.handler = handler
        
        self.inputs = inputs
        self.dependencies = dependencies

        self.select_group = select_group
        self.select_result = select_result
            
        self.expected_outputs = expected_outputs
        
        self.save_continuation = save_continuation
        
        self.replay_uuids = replay_uuids

    def __repr__(self):
        return 'Task(%s)' % self.task_id

class TaskPoolTask(Task):
    
    def __init__(self, task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation=False, continues_task=None, replay_uuids=None, select_group=None, select_result=None, state=TASK_CREATED, task_pool=None, job=None):
        Task.__init__(self, task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation, continues_task, replay_uuids, select_group, select_result, state)
        
        self.task_pool = task_pool
        
        self.unfinished_input_streams = set()

        self._blocking_dict = {}
        if select_group is not None:
            self._selecting_dict = {}
            
        self.history = []
        
        self.job = job
        
        self.set_state(state)
        
        self.worker = None
        self.saved_continuation_uri = None

        
        self.event_index = 0
        self.current_attempt = 0

    def set_state(self, state):
        if self.job is not None:
            self.job.record_state_change(self.state, state)
        self.record_event(TASK_STATE_NAMES[state])
        self.state = state
        if state in (TASK_COMMITTED, TASK_ASSIGNED):
            evt_time = self.history[-1][0]
            cherrypy.log('%s %s %s @ %f' % (self.task_id, TASK_STATE_NAMES[self.state], self.worker.id if self.worker is not None else 'None', time.mktime(evt_time.timetuple()) + evt_time.microsecond / 1e6), 'TASK', logging.INFO)
        #cherrypy.log('Task %s: --> %s' % (self.task_id, TASK_STATE_NAMES[self.state]), 'TASK', logging.INFO)
        
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
                        self.block_on(global_id, local_id)
                else:
                    self.inputs[local_id] = input
    
            if len(self._blocking_dict) > 0:
                self.set_state(TASK_BLOCKING)
            else:
                self.set_state(TASK_RUNNABLE)
                
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
                    self.set_state(TASK_SELECTING)
                else:
                    self.set_state(TASK_RUNNABLE)
        
            else:
                
                # We are replaying a task that has previously returned from a call to select().
                # TODO: We need to make sure we handle blocking/failure for this task correctly.
                self.set_state(TASK_RUNNABLE)
        
    def is_replay_task(self):
        return self.replay_ref is not None
        
    def is_blocked(self):
        return self.state in (TASK_BLOCKING, TASK_SELECTING)
    
    def is_assigned_streaming(self):
        return self.state == TASK_ASSIGNED_STREAMING

    def is_queued_streaming(self):
        return self.state == TASK_QUEUED_STREAMING
        
    def blocked_on(self):
        if self.state == TASK_SELECTING:
            return self._selecting_dict.keys()
        elif self.state == TASK_BLOCKING:
            return self._blocking_dict.keys()
        else:
            return []

    def block_on(self, global_id, local_id):
        self.set_state(TASK_BLOCKING)
        try:
            self._blocking_dict[global_id].add(local_id)
        except KeyError:
            self._blocking_dict[global_id] = set([local_id])
            
    def notify_reference_changed(self, global_id, ref):
        if global_id in self.unfinished_input_streams:
            self.unfinished_input_streams.remove(global_id)
            self.task_pool.unsubscribe_task_from_ref(self, ref)
            if len(self.unfinished_input_streams) == 0:
                if self.state == TASK_ASSIGNED_STREAMING:
                    self.set_state(TASK_ASSIGNED)
                elif self.state == TASK_QUEUED_STREAMING:
                    self.set_state(TASK_QUEUED)
        else:
            if self.state == TASK_BLOCKING:
                local_ids = self._blocking_dict.pop(global_id)
                for local_id in local_ids:
                    self.inputs[local_id] = ref
                if isinstance(ref, SW2_StreamReference):
                    # Stay subscribed; this ref is still interesting
                    self.unfinished_input_streams.add(global_id)
                else:
                    # Don't need to hear about this again
                    self.task_pool.unsubscribe_task_from_ref(self, ref)
                if len(self._blocking_dict) == 0:
                    self.set_state(TASK_RUNNABLE)
        
    # Warning: called under worker_pool._lock
    def set_assigned_to_worker(self, worker):
        self.worker = worker
        if len(self.unfinished_input_streams) > 0:
            self.set_state(TASK_ASSIGNED_STREAMING)
        else:
            self.set_state(TASK_ASSIGNED)
        #self.state = TASK_ASSIGNED
        #self.record_event("ASSIGNED")
        #self.task_pool.notify_task_assigned_to_worker_id(self, worker_id)

    def convert_dependencies_to_futures(self):
        new_deps = {}
        for local_id, ref in self.dependencies.items(): 
            new_deps[local_id] = ref.as_future()
        self.dependencies = new_deps

    def make_replay_task(self, replay_task_id, replay_ref):
        
        ret = TaskPoolTask(replay_task_id, self.parent, self.handler, self.inputs, self.dependencies, self.expected_outputs, self.save_continuation, self.continues_task, self.replay_uuids, self.select_group, self.select_result, TASK_RUNNABLE, self.task_pool)
        ret.original_task_id = self.task_id
        ret.replay_ref = replay_ref
        return ret

    def as_descriptor(self, long=False):        
        descriptor = {'task_id': self.task_id,
                      'dependencies': self.dependencies,
                      'handler': self.handler,
                      'expected_outputs': self.expected_outputs,
                      'inputs': self.inputs,
                      'event_index': self.event_index}

        descriptor['parent'] = self.parent.task_id if self.parent is not None else None
        
        if long:
            descriptor['history'] = map(lambda (t, name): (time.mktime(t.timetuple()) + t.microsecond / 1e6, name), self.history)
            if self.worker is not None:
                descriptor['worker_id'] = self.worker.id
            descriptor['saved_continuation_uri'] = self.saved_continuation_uri
            descriptor['state'] = TASK_STATE_NAMES[self.state]
            descriptor['children'] = [x.task_id for x in self.children]
                    
        if self.select_result is not None:
            descriptor['select_result'] = self.select_result
        
        if self.save_continuation:
            descriptor['save_continuation'] = True
        if self.continues_task is not None:
            descriptor['continues_task'] = self.continues_task
        if self.continuation is not None:
            descriptor['continuation'] = self.continuation
        if self.replay_uuids is not None:
            descriptor['replay_uuids'] = self.replay_uuids
            
        if self.original_task_id is not None:
            descriptor['original_task_id'] = self.original_task_id
        if self.replay_ref is not None:
            descriptor['replay_ref'] = self.replay_ref
        
        return descriptor        


def build_taskpool_task_from_descriptor(task_id, task_descriptor, task_pool, parent_task=None):

    handler = task_descriptor['handler']
    
    try:
        inputs = task_descriptor['inputs']
    except KeyError:
        inputs = {}
        
    dependencies = task_descriptor['dependencies']
    expected_outputs = task_descriptor['expected_outputs']

    try:
        save_continuation = task_descriptor['save_continuation']
    except KeyError:
        save_continuation = False

    try:
        continues_task = task_descriptor['continues_task']
    except KeyError:
        continues_task = None

    try:
        select_group = task_descriptor['select_group']
    except:
        select_group = None
    select_result = None

    replay_uuids = None
    
    state = TASK_CREATED
    
    return TaskPoolTask(task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation, continues_task, replay_uuids, select_group, select_result, state, task_pool)
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
