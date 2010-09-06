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
'''
Created on 15 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from cherrypy.process import plugins
from threading import Condition, RLock
from skywriting.runtime.references import \
    SWURLReference, SWErrorReference, SW2_ConcreteReference,\
    SWTaskOutputProvenance, SWSpawnedTaskProvenance,\
    SWTaskContinuationProvenance, SWExecResultProvenance,\
    SWSpawnExecArgsProvenance, SW2_FutureReference
from skywriting.runtime.task import TASK_RUNNABLE, TASK_ABORTED,\
    TASK_COMMITTED, TASK_ASSIGNED, TASK_FAILED,\
    build_taskpool_task_from_descriptor, TASK_QUEUED
from skywriting.runtime.block_store import get_netloc_for_sw_url
import logging
import uuid
import cherrypy

class TaskPool(plugins.SimplePlugin):
    
    def __init__(self, bus, global_name_directory, worker_pool, deferred_worker):
        plugins.SimplePlugin.__init__(self, bus)
        self.global_name_directory = global_name_directory
        self.worker_pool = worker_pool
        self.deferred_worker = deferred_worker
        self.current_task_id = 0
        self.tasks = {}
        self.references_blocking_tasks = {}
        self._lock = RLock()
        self._cond = Condition(self._lock)
        self.max_concurrent_waiters = 5
        self.current_waiters = 0
        self.event_index = 0
        # event_index: The index which will be given to the *next* event
        self.events = []
        self.is_stopping = False
        
    # Call under _lock (and don't release _lock until you've put an event in the queue!)
    def new_event(self, t):
        ret = dict()
        ret["index"] = self.event_index
        ret["task_id"] = t.task_id
        t.event_index = self.event_index
        self.event_index += 1
        self._cond.notify_all()
        return ret

    def wait_event_after(self, idx):
        with self._lock:
            self.current_waiters = self.current_waiters + 1
            while idx == self.event_index:
                if self.current_waiters > self.max_concurrent_waiters:
                    break
                elif self.is_stopping:
                    break
                else:
                    self._cond.wait()
            self.current_waiters = self.current_waiters - 1
            if self.is_stopping:
                raise Exception("Server stopping")
            elif self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")

    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            self._cond.notify_all()

    def subscribe(self):
        self.bus.subscribe('global_name_available', self.reference_available)
        self.bus.subscribe('task_failed', self.task_failed)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        # Stop method gets run before the HTTP server
    
    def unsubscribe(self):
        self.bus.unsubscribe('global_name_available', self.reference_available)
        self.bus.unsubscribe('task_failed', self.task_failed)
        self.bus.unsubscribe('stop', self.server_stopping)
    
    def compute_best_worker_for_task(self, task):
        netlocs = {}
        for input in task.inputs.values():
            if isinstance(input, SWURLReference):
                if input.size_hint is None:
                    input.size_hint = 10000000
                    # Do something sensible here; probably HTTP HEAD
                for url in input.urls:
                    netloc = get_netloc_for_sw_url(url)
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
            ret = self.worker_pool.get_worker_at_netloc(max(filtered_ranked_netlocs)[1])
            return ret
        else:
            return None
    
    def add_task_to_queues(self, task):
        # TODO: Compute best worker(s) here.
        best_worker = self.compute_best_worker_for_task(task)
        task.state = TASK_QUEUED
        if best_worker is not None:
            best_worker.local_queue.put(task)
        handler_queue = self.worker_pool.feature_queues.get_queue_for_feature(task.handler)
        handler_queue.put(task)
    
    def generate_task_id(self):
        return str(uuid.uuid1())
    
    def add_task(self, task_descriptor, parent_task_id=None):
        with self._lock:
            try:
                task_id = task_descriptor['task_id']
            except:
                task_id = self.generate_task_id()
            
            task = build_taskpool_task_from_descriptor(task_id, task_descriptor, self, parent_task_id)
            self.tasks[task_id] = task
            add_event = self.new_event(task)
            add_event["task_descriptor"] = task.as_descriptor(long=True)
            add_event["action"] = "CREATED"
        
            task.check_dependencies(self.global_name_directory)

            if task.is_blocked():
                for global_id in task.blocked_on():
                    try:
                        self.references_blocking_tasks[global_id].add(task_id)
                    except KeyError:
                        self.references_blocking_tasks[global_id] = set([task_id])
            else:
                task.state = TASK_RUNNABLE
                self.add_task_to_queues(task)

            self.events.append(add_event)
                
        self.bus.publish('schedule')
        return task

    # Warning: called under worker_pool._lock
    def notify_task_assigned_to_worker_id(self, task, worker_id):
        with self._lock:
            assigned_event = self.new_event(task)
            assigned_event["action"] = "ASSIGNED"
            assigned_event["worker_id"] = worker_id
            self.events.append(assigned_event)

    def _mark_task_as_aborted(self, task_id):
        task = self.tasks[task_id]
        previous_state = task.state
        task.state = TASK_ABORTED
        with self._lock:
            abort_event = self.new_event(task)
            abort_event["action"] = "ABORTED"
            self.events.append(abort_event)
        return task, previous_state

    def _abort(self, task_id):
        task, previous_state = self._mark_task_as_aborted(task_id)
        if previous_state == TASK_ASSIGNED:
            task.record_event("ABORTED")
            self.worker_pool.abort_task_on_worker(task)
        for child in task.children:
            self._abort(child)
        
    def abort(self, task_id):
        with self._lock:
            self._abort(task_id)
            
    def reference_available(self, id, refs):

        with self._lock:

            try:
                blocked_tasks_set = self.references_blocking_tasks.pop(id)
            except KeyError:
                return
            for task_id in blocked_tasks_set:
                task = self.tasks[task_id]
                was_blocked = task.is_blocked()
                task.unblock_on(id, refs)
                if was_blocked and not task.is_blocked():

                    runnable_event = self.new_event(task)
                    runnable_event["action"] = "RUNNABLE"
                    self.events.append(runnable_event)
                    task.state = TASK_RUNNABLE
                    task.record_event("RUNNABLE")
                    self.add_task_to_queues(task)
                    self.bus.publish('schedule')
                    
    def get_task_by_id(self, id):
        return self.tasks[id]
    
    def task_completed(self, id):
        
        with self._lock:
            task = self.tasks[id]
            worker_id = task.worker_id
            committed_event = self.new_event(task)
            committed_event["action"] = "COMMITTED"
            self.events.append(committed_event)
            task.state = TASK_COMMITTED
            task.record_event("COMMITTED")
            
        self.bus.publish('worker_idle', worker_id)

    def handle_missing_input_failure(self, task, input_ref):
        task.record_event("MISSING_INPUT_FAILURE")

        print 'Handling missing input failure for:', task, input_ref

        # Inform global data store that the referenced data is being reproduced.
        # TODO: We may have already started to reproduce this data, so we should check that.
        self.global_name_directory.delete_all_refs_for_id(input_ref.id)

        additional_id_ref_mappings = []

        # Rewrite previously-concrete references to the missing input to be futures.
        for (local_id, ref) in task.dependencies.items():
            if isinstance(ref, SW2_ConcreteReference) and ref.id == input_ref.id:
                additional_id_ref_mappings.append((local_id, SW2_FutureReference(ref.id, input_ref.provenance)))

        for (local_id, ref) in additional_id_ref_mappings:
            task.dependencies[local_id] = ref

        # Requeue the current task.
        task.check_dependencies(self.global_name_directory)
        if task.is_blocked():
            for global_id in task.blocked_on():
                try:
                    self.references_blocking_tasks[global_id].add(task.task_id)
                except KeyError:
                    self.references_blocking_tasks[global_id] = set([task.task_id])
        else:
            task.state = TASK_RUNNABLE
            self.add_task_to_queues(task)

        # Replicate the task that produced the missing input (if not already done).
        if isinstance(input_ref.provenance, SWTaskOutputProvenance):
            # 1. Find the (original) task that should have written this output, from the provenance.
            producing_task = self.tasks[input_ref.provenance.task_id]
            
            # 2. Iterate through the continuation chain until we find the task that actually wrote it.
            while producing_task.continuation is not None:
                producing_task = self.tasks[producing_task.continuation]
            
            # 3. Re-execute that task, and publish the relevant outputs.
            replay_task = producing_task.make_replay_task(self.generate_task_id(), input_ref)
            
        elif isinstance(input_ref.provenance, SWSpawnedTaskProvenance):
            # 1. Find the task that spawned this task, from the provenance.
            producing_task = self.tasks[input_ref.provenance.task_id]
            
            # 2. Re-execute that task, and ensure that the spawned continuation gets published at the end.
            replay_task = producing_task.make_replay_task(self.generate_task_id(), input_ref)

        elif isinstance(input_ref.provenance, SWTaskContinuationProvenance):
            # 1. Find the task that wrote this output, from the provenance.
            producing_task = self.tasks[input_ref.provenance.task_id]
            
            # 2. Re-execute that task, and ensure that the continuation gets published at the end.
            replay_task = producing_task.make_replay_task(self.generate_task_id(), input_ref)
            
        elif isinstance(input_ref.provenance, SWExecResultProvenance):
            # 1. Find the task that did this exec, from the provenance.
            producing_task = self.tasks[input_ref.provenance.task_id]
            
            # 2. Re-execute that task, and ensure that the exec result gets published at the end.
            replay_task = producing_task.make_replay_task(self.generate_task_id(), input_ref)

        elif isinstance(input_ref.provenance, SWSpawnExecArgsProvenance):
            # 1. Find the task that did this exec, from the provenance.
            producing_task = self.tasks[input_ref.provenance.task_id]
            
            # 2. Re-execute that task, and ensure that the spawn_exec args get published at the end.
            replay_task = producing_task.make_replay_task(self.generate_task_id(), input_ref)

        # Investigate the failure of the workers that should have provided this data.
        for netloc in input_ref.location_hints:
            worker = self.worker_pool.get_worker_at_netloc(netloc)
            print 'Investigating failure of worker:', worker
            if worker is not None:
                self.deferred_worker.do_deferred(lambda: self.worker_pool.investigate_worker_failure(worker))

        self.tasks[replay_task.task_id] = replay_task
        self.add_task_to_queues(replay_task)
        
        self.bus.publish('schedule')
    
    
    def task_failed(self, id, reason, details=None):
        cherrypy.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        worker_id = None
        should_notify_outputs = False

        with self._lock:
            task = self.tasks[id]
            failure_event = self.new_event(task)
            if reason == 'WORKER_FAILED':
                # Try to reschedule task.
                task.current_attempt += 1
                task.record_event("WORKER_FAILURE")
                if task.current_attempt > 3:
                    task.state = TASK_FAILED
                    task.record_event("TASK_FAILURE")
                    failure_event["action"] = "WORKER_FAIL"
                    should_notify_outputs = True
                else:
                    self.add_task_to_queues(task)
                    self.bus.publish('schedule')
                    failure_event["action"] = "WORKER_FAIL_RETRY"
                    
            elif reason == 'MISSING_INPUT':
                # Problem fetching input, so we will have to reexecute it.
                worker_id = task.worker_id
                failure_event["action"] = "MISSING_INPUT_FAIL"
                self.handle_missing_input_failure(task, details)
                
            elif reason == 'RUNTIME_EXCEPTION':
                # Kill the entire job, citing the problem.
                worker_id = task.worker_id
                task.record_event("RUNTIME_EXCEPTION_FAILURE")
                task.state = TASK_FAILED
                failure_event["action"] = "RUNTIME_EXCEPTION_FAIL"
                should_notify_outputs = True
            
            self.events.append(failure_event)

        # Doing this outside the lock because this leads via add_refs_to_id
        # --> self::reference_available, creating a circular wait. We noted the task as FAILED inside the lock,
        # which ought to be enough.
        if should_notify_outputs:
            for output in task.expected_outputs:
                self.global_name_directory.add_refs_for_id(output, [SWErrorReference(reason, details)]) 

        if worker_id is not None:
            self.bus.publish('worker_idle', worker_id)

    def spawn_child_tasks(self, parent_task, spawned_task_descriptors):
        # TODO: stage this in a task-local transaction buffer.
        
        if parent_task.is_replay_task():
            return
            
        for child in spawned_task_descriptors:
            try:
                spawned_task_id = child['task_id']
                expected_outputs = child['expected_outputs']
                for global_id in expected_outputs:
                    self.global_name_directory.create_global_id(spawned_task_id, global_id)
                
                # XXX: We expect the task descriptor to contain UUID objects, not strings.
                child['expected_outputs'] = expected_outputs
            except KeyError:
                raise
            
            task = self.add_task(child, parent_task.task_id)
            parent_task.children.append(task.task_id)
            
            if task.continues_task is not None:
                parent_task.continuation = spawned_task_id

    def commit_task(self, task_id, commit_payload):
        
        commit_bindings = commit_payload['bindings']
        task = self.get_task_by_id(task_id)
        
        # The list of UUIDs generated by this task (for replay purposes).
        try:
            replay_uuid_list = commit_payload['replay_uuids']
            task.replay_uuids = replay_uuid_list
        except KeyError:
            pass
        
        # Apply commit bindings (if any), i.e. publish results.
        for global_id, refs in commit_bindings.items():
            self.global_name_directory.add_refs_for_id(global_id, refs)
        
        self.task_completed(task_id)
        
        # Saved continuation URI, if necessary.
        try:
            commit_continuation_uri = commit_payload['saved_continuation_uri']
            task.saved_continuation_uri = commit_continuation_uri
        except KeyError:
            pass

        
    def flush_task_dict(self):
        cherrypy.log.error("Flushing tasks dict. In-progress jobs will fail.", "TASK", logging.WARN, False)
        self.tasks = {}
        self.references_blocking_tasks = {}
