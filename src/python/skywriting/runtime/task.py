'''
Created on 17 Aug 2010

@author: dgm36
'''
import datetime
from shared.references import SW2_FutureReference, SW2_StreamReference,\
    SW2_FixedReference

import time
import ciel
import logging
import shared

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

class TaskPoolTask:
    
    def __init__(self, task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation=False, continues_task=None, task_private=None, replay_uuids=None, select_group=None, select_result=None, state=TASK_CREATED, job=None, taskset=None, worker_private=None, workers=[]):
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
        self.task_private = task_private
        
        self.replay_uuids = replay_uuids
        self.unfinished_input_streams = set()

        self.constrained_location_checked = False
        self.constrained_location = None

        self._blocking_dict = {}
        if select_group is not None:
            self._selecting_dict = {}
            
        self.history = []
        
        self.job = job
        self.taskset = taskset
        
        self.worker_private = worker_private
        
        self.state = None
        self.set_state(state)
        
        #self.worker = None
        self.workers = set(workers)
        
        self.saved_continuation_uri = None

        self.event_index = 0
        self.current_attempt = 0

    def set_state(self, state):
        if self.job is not None and self.state is not None:
            self.job.record_state_change(self.state, state)
        self.record_event(TASK_STATE_NAMES[state])
        self.state = state
        if state in (TASK_COMMITTED, TASK_ASSIGNED):
            evt_time = self.history[-1][0]
            ciel.log('%s %s @ %f' % (self.task_id, TASK_STATE_NAMES[self.state], time.mktime(evt_time.timetuple()) + evt_time.microsecond / 1e6), 'TASK', logging.INFO)
        #ciel.log('Task %s: --> %s' % (self.task_id, TASK_STATE_NAMES[self.state]), 'TASK', logging.INFO)
        
    def record_event(self, description, time=None):
        if time is None:
            time = datetime.datetime.now()
        self.history.append((time, description))
        
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

    def set_profiling(self, profiling):
        self.profiling = profiling
    
        ordered_events = [(timestamp, event) for (event, timestamp) in profiling.items()]
        ordered_events.sort()
        
        for timestamp, event in ordered_events:
            self.record_event(event, datetime.datetime.fromtimestamp(timestamp))
    
    def get_profiling(self):
        try:
            return self.profiling
        except AttributeError:
            return {}

    def assign_netloc(self, netloc):
        self.workers.add(netloc)

    def delete_netloc(self, netloc):
        self.workers.remove(netloc)

    def get_netlocs(self):
        """Returns a list of network locations representing the workers on which this task is running."""
        return list(self.workers)

    def block_on(self, global_id, local_id):
        self.set_state(TASK_BLOCKING)
        try:
            self._blocking_dict[global_id].add(local_id)
        except KeyError:
            self._blocking_dict[global_id] = set([local_id])
            
    def notify_reference_changed(self, global_id, ref, task_pool):
        if global_id in self.unfinished_input_streams:
            self.unfinished_input_streams.remove(global_id)
            task_pool.unsubscribe_task_from_ref(self, ref)
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
                    task_pool.unsubscribe_task_from_ref(self, ref)
                if len(self._blocking_dict) == 0:
                    self.set_state(TASK_RUNNABLE)

    def notify_ref_table_updated(self, ref_table_entry):
        global_id = ref_table_entry.ref.id
        ref = ref_table_entry.ref
        if global_id in self.unfinished_input_streams:
            self.unfinished_input_streams.remove(global_id)
            ref_table_entry.remove_consumer(self)
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
                    ref_table_entry.remove_consumer(self)
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

    def has_constrained_location(self):
        for dep in self.dependencies.values():
            if isinstance(dep, SW2_FixedReference):
                self.constrained_location = dep.fixed_netloc
        self.constrained_location_checked = True
                
    def get_constrained_location(self):
        if not self.constrained_location_checked:
            self.has_constrained_location()
        return self.constrained_location

    def as_descriptor(self, long=False):        
        descriptor = {'task_id': self.task_id,
                      'dependencies': self.dependencies.values(),
                      'handler': self.handler,
                      'expected_outputs': self.expected_outputs,
                      'inputs': self.inputs.values(),
                      'event_index': self.event_index,
                      'job' : self.job.id}

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
        if self.task_private is not None:
            descriptor['task_private'] = self.task_private
            
        if self.original_task_id is not None:
            descriptor['original_task_id'] = self.original_task_id
        if self.replay_ref is not None:
            descriptor['replay_ref'] = self.replay_ref
        
        return descriptor        

    def as_protobuf_for_execution(self):
        '''
        This version treats self.inputs as the official set of (concrete)
        dependencies.
        '''
        pass


    try:    
        
        from shared.generated.ciel.protoc_pb2 import Task
            
        def as_protobuf_for_spawning(self):
            '''
            This version treats self.dependencies as the official set of (perhaps)
            future dependencies.
            '''
            task = Task()
            task.id = self.task_id
            task.executor = Task.DESCRIPTOR.enum_types_by_name['ExecutorType'].values_by_name[self.handler]

            task.task_private = self.task_private.as_protobuf()
            
            for dep in self.inputs.values():
                task.dependencies.add(dep.as_protobuf())

            for expected_output in self.expected_outputs:
                task.expected_outputs.add(expected_output)

            return task

    except ImportError:
        pass
        
try:
    from shared.generated.ciel.protoc_pb2 import Task
    from shared.references import build_reference_from_protobuf
    
    def build_taskpool_task_from_protobuf(buf_task, parent_task=None):
        
        task_id = buf_task.id
        
        # Ignored fields.
        inputs = {}
        save_continuation = False
        continues_task = None
        replay_uuids = None
        select_group = None
        select_result = None
        
        state = TASK_CREATED
        
        # XXX: This is how we get the name of an enum value as a Python string. Yuck.
        handler = Task.DESCRIPTOR.enum_types_by_name['ExecutorType'].values_by_number[buf_task.executor]
        
        task_private = build_reference_from_protobuf(buf_task.task_private)
        
        dependencies = {}
        for buf_ref in buf_task.dependencies:
            dependencies[buf_ref.id] = build_reference_from_protobuf(buf_ref)
            
        expected_outputs = []
        for expected_output in buf_ref.expected_outputs:
            expected_outputs.append(expected_output)
            
        return TaskPoolTask(task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation, continues_task, task_private, replay_uuids, select_group, select_result, state)

    def build_protobuf_task_from_worker_task_descriptor(task_descriptor):
        return build_taskpool_task_from_descriptor(task_descriptor).as_protobuf_for_spawning()
        
                        
except ImportError:
    pass


class DummyJob:
    """Used to ensure that tasks on the worker can refer to their job (for inheriting job ID, e.g.)."""
    
    def __init__(self, id):
        self.id = id
        
    def record_state_change(self, from_state, to_state):
        pass

def build_taskpool_task_from_descriptor(task_descriptor, parent_task=None, taskset=None):

    task_id = task_descriptor['task_id']

    handler = task_descriptor['handler']
    
    if parent_task is not None:
        job = parent_task.job
    else:
        try:
            job = DummyJob(task_descriptor['job'])
        except KeyError:
            job = DummyJob(None)
    
    try:
        inputs = dict([(ref.id, ref) for ref in task_descriptor['inputs']])
    except KeyError:
        inputs = {}
        
    dependencies = dict([(ref.id, ref) for ref in task_descriptor['dependencies']])
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
        task_private = task_descriptor['task_private']
    except KeyError:
        task_private = None

    try:
        select_group = task_descriptor['select_group']
    except:
        select_group = None
    select_result = None

    try:
        worker_private = task_descriptor['worker_private']
    except:
        worker_private = {}

    try:
        workers = task_descriptor['workers']
    except:
        workers = []

    replay_uuids = None
    
    state = TASK_CREATED
    
    return TaskPoolTask(task_id, parent_task, handler, inputs, dependencies, expected_outputs, save_continuation, continues_task, task_private, replay_uuids, select_group, select_result, state, job, taskset, worker_private, workers)
