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

from Queue import Queue
from cherrypy.process import plugins
from skywriting.runtime.master.job_pool import Job
from shared.references import SW2_FutureReference, \
    SWErrorReference, combine_references, SW2_StreamReference
from skywriting.runtime.task import TASK_CREATED, TASK_BLOCKING, TASK_RUNNABLE, \
    TASK_COMMITTED, build_taskpool_task_from_descriptor, TASK_QUEUED, TASK_FAILED, TASK_QUEUED_STREAMING
from threading import Lock
import collections
import logging
import uuid
import ciel

class LazyTaskPool(plugins.SimplePlugin):
    
    def __init__(self, bus, worker_pool):
    
        # Used for publishing schedule events.
        self.bus = bus

        # For signalling workers of task state changes
        self.worker_pool = worker_pool
    
        # Mapping from task ID to task object.
        self.tasks = {}
        
        # Mapping from expected output to producing task.
        self.task_for_output = {}
        
        # Mapping from expected output to consuming tasks.
        self.consumers_for_output = {}
        
        # Mapping from output name to concrete reference.
        self.ref_for_output = {}
        
        # Current set of job outputs: i.e. expected outputs that we want to
        # produce by lazy graph reduction.
        self.job_outputs = {}
        
        # A thread-safe queue of runnable tasks, which we use to pass tasks to
        # the LazyScheduler.
        self.task_queue = Queue()
        
        # At the moment, this is a coarse-grained lock, which is acquired when
        # a task is added or completed, or when references are externally
        # published.
        self._lock = Lock()
        
    def subscribe(self):
        self.bus.subscribe('task_failed', self.task_failed)
        
    def unsubscribe(self):
        self.bus.unsubscribe('task_failed', self.task_failed)
        
    def get_task_by_id(self, task_id):
        return self.tasks[task_id]
        
    def get_ref_by_id(self, ref_id):
        with self._lock:
            return self.ref_for_output[ref_id]
        
    def get_reference_info(self, id):
        with self._lock:
            ref = self.ref_for_output[id]
            try:
                consumers = self.consumers_for_output[id]
            except KeyError:
                consumers = []
            task = self.task_for_output[id]
            return {'ref': ref, 'consumers': list(consumers), 'task': task.as_descriptor()}
        
    def add_task(self, task, is_root_task=False, may_reduce=True):

        if task.task_id not in self.tasks:
            self.tasks[task.task_id] = task
            should_register = True
        else:
            ciel.log('Already seen task %s: do not register its outputs' % task.task_id, 'TASKPOOL', logging.INFO)
            should_register = False
        
        if is_root_task:
            self.job_outputs[task.expected_outputs[0]] = task.job
            ciel.log('Registering job (%s) interest in output (%s)' % (task.job.id, task.expected_outputs[0]), 'TASKPOOL', logging.INFO)
            self.register_job_interest_for_output(task.expected_outputs[0], task.job)
        
        task.job.add_task(task)
        
        # If any of the task outputs are being waited on, we should reduce this
        # task's graph. 
        with self._lock:
            if should_register:
                should_reduce = self.register_task_outputs(task) and may_reduce
                if should_reduce:
                    self.do_graph_reduction(root_tasks=[task])
                elif is_root_task:
                    self.do_graph_reduction(root_tasks=[task])
            elif is_root_task:
                ciel.log('Reducing graph from roots.', 'TASKPOOL', logging.INFO)
                self.do_root_graph_reduction()
            
    def task_completed(self, task, commit_bindings, should_publish=True):
        task.set_state(TASK_COMMITTED)
        
        # Need to notify all of the consumers, which may make other tasks
        # runnable.
        self.publish_refs(commit_bindings, task.job, task=task)
        if task.worker is not None and should_publish:
            self.bus.publish('worker_idle', task.worker)
        
    def get_task_queue(self):
        return self.task_queue
        
    def task_failed(self, task, payload):

        (reason, details, bindings) = payload

        ciel.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        worker = None
        should_notify_outputs = False

        task.record_event(reason)


        self.publish_refs(bindings, task.job)

        with self._lock:
            if reason == 'WORKER_FAILED':
                # Try to reschedule task.
                task.current_attempt += 1
                # XXX: Remove this hard-coded constant. We limit the number of
                #      retries in case the task is *causing* the failures.
                if task.current_attempt > 3:
                    task.set_state(TASK_FAILED)
                    should_notify_outputs = True
                else:
                    ciel.log.error('Rescheduling task %s after worker failure' % task.task_id, 'TASKPOOL', logging.WARNING)
                    task.set_state(TASK_FAILED)
                    self.add_runnable_task(task)
                    self.bus.publish('schedule')
                    
            elif reason == 'MISSING_INPUT':
                # Problem fetching input, so we will have to re-execute it.
                worker = task.worker
                for binding in bindings.values():
                    ciel.log('Missing input: %s' % str(binding), 'TASKPOOL', logging.WARNING)
                self.handle_missing_input(task)
                
            elif reason == 'RUNTIME_EXCEPTION':
                # A hard error, so kill the entire job, citing the problem.
                worker = task.worker
                task.set_state(TASK_FAILED)
                should_notify_outputs = True

        # Doing this outside the lock because this leads via add_refs_to_id
        # --> self::reference_available, creating a circular wait. We noted the task as FAILED inside the lock,
        # which ought to be enough.
        if should_notify_outputs:
            for output in task.expected_outputs:
                self._publish_ref(output, SWErrorReference(output, reason, details), task.job)

        if worker is not None:
            self.bus.publish('worker_idle', worker)
    
    def handle_missing_input(self, task):
        task.set_state(TASK_FAILED)
                
        # Assume that all of the dependencies are unavailable.
        task.convert_dependencies_to_futures()
        
        # We will re-reduce the graph for this task, ignoring the network
        # locations for which getting the input failed.
        # N.B. We should already have published the necessary tombstone refs
        #      for the failed inputs.
        self.do_graph_reduction(root_tasks=[task])
    
    def publish_single_ref(self, global_id, ref, job, should_journal=True, task=None):
        with self._lock:
            self._publish_ref(global_id, ref, job, should_journal, task)
    
    def publish_refs(self, refs, job=None, should_journal=True, task=None):
        with self._lock:
            for global_id, ref in refs.items():
                self._publish_ref(global_id, ref, job, should_journal, task)
        
    def _publish_ref(self, global_id, ref, job=None, should_journal=True, task=None):
        
        if should_journal and job is not None:
            job.add_reference(global_id, ref)
        
        # Record the name-to-concrete-reference mapping for this ref's name.
        try:
            combined_ref = combine_references(self.ref_for_output[global_id], ref)
            if not combined_ref:
                return
            if not combined_ref.is_consumable():
                del self.ref_for_output[global_id]
                return
            self.ref_for_output[global_id] = combined_ref
        except KeyError:
            if ref.is_consumable():
                self.ref_for_output[global_id] = ref
            else:
                return

        current_ref = self.ref_for_output[global_id]

        if task is not None:
            self.task_for_output[global_id] = task

        # Notify any consumers that the ref is now available.
        # Contrary to how this was in earlier versions, tasks must unsubscribe themselves.
        # I always unsubscribe Jobs for simplicity, and because Jobs never need more than one callback.
        try:
            consumers = self.consumers_for_output[global_id]
            iter_consumers = consumers.copy()
            # Avoid problems with deletion from set during iteration
            for consumer in iter_consumers:
                if isinstance(consumer, Job) and consumer.job_pool is not None:
                    consumer.job_pool.job_completed(consumer, current_ref)
                    self.unregister_job_interest_for_output(current_ref.id, consumer)
                else:
                    self.notify_task_of_reference(consumer, global_id, current_ref)
        except KeyError:
            pass

    def notify_task_of_reference(self, task, id, ref):
        if ref.is_consumable():
            was_queued_streaming = task.is_queued_streaming()
            was_blocked = task.is_blocked()
            task.notify_reference_changed(id, ref, self)
            if was_blocked and not task.is_blocked():
                self.add_runnable_task(task)
            elif was_queued_streaming and not task.is_queued_streaming():
                # Submit this to the scheduler again
                self.add_runnable_task(task)

    def register_job_interest_for_output(self, ref_id, job):
        try:
            subscribers = self.consumers_for_output[ref_id]
        except:
            subscribers = set()
            self.consumers_for_output[ref_id] = subscribers
        subscribers.add(job)

    def unregister_job_interest_for_output(self, ref_id, job):
        try:
            subscribers = self.consumers_for_output[ref_id]
            subscribers.remove(job)
            if len(subscribers) == 0:
                del self.consumers_for_output[ref_id]
        except:
            ciel.log.error("Job %s failed to unsubscribe from ref %s" % (job, ref_id), "TASKPOOL", logging.WARNING)

    def subscribe_task_to_ref(self, task, ref):
        try:
            subscribers = self.consumers_for_output[ref.id]
        except:
            subscribers = set()
            self.consumers_for_output[ref.id] = subscribers
        subscribers.add(task)

    def unsubscribe_task_from_ref(self, task, ref):
        try:
            subscribers = self.consumers_for_output[ref.id]
            subscribers.remove(task)
            if len(subscribers) == 0:
                del self.consumers_for_output[ref.id]
        except:
            ciel.log.error("Task %s failed to unsubscribe from ref %s" % (task, ref.id), "TASKPOOL", logging.WARNING)
            
    def register_task_interest_for_ref(self, task, ref):
        if isinstance(ref, SW2_FutureReference):
            # First, see if we already have a concrete reference for this
            # output.
            try:
                conc_ref = self.ref_for_output[ref.id]
                return conc_ref
            except KeyError:
                pass
            
            # Otherwise, subscribe to the production of the named output.
            self.subscribe_task_to_ref(task, ref)
            return None
        
        else:
            # We have an opaque reference, which can be accessed immediately.
            return ref
        
    def register_task_outputs(self, task):
        # If any tasks have previously registered an interest in any of this
        # task's outputs, we need to reduce the given task.
        should_reduce = False
        for output in task.expected_outputs:
            self.task_for_output[output] = task
            if self.output_has_consumers(output):
                should_reduce = True
        return should_reduce
    
    def output_has_consumers(self, output):
        try:
            subscribers = self.consumers_for_output[output]
            return len(subscribers) > 0
        except KeyError:
            return False
    
    def add_runnable_task(self, task):
        if len(task.unfinished_input_streams) == 0:
            task.set_state(TASK_QUEUED)
        else:
            task.set_state(TASK_QUEUED_STREAMING)
        self.task_queue.put(task)
    
    def do_root_graph_reduction(self):
        self.do_graph_reduction(object_ids=self.job_outputs.keys())
    
    def do_graph_reduction(self, object_ids=[], root_tasks=[]):
        
        should_schedule = False
        newly_active_task_queue = collections.deque()
        
        # Initially, start with the root set of tasks, based on the desired
        # object IDs.
        for object_id in object_ids:
            try:
                if self.ref_for_output[object_id].is_consumable():
                    continue
            except KeyError:
                pass
            task = self.task_for_output[object_id]
            if task.state == TASK_CREATED:
                # Task has not yet been scheduled, so add it to the queue.
                task.set_state(TASK_BLOCKING)
                newly_active_task_queue.append(task)
            
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
                conc_ref = self.register_task_interest_for_ref(task, 
                                                               ref)
                if conc_ref is not None and conc_ref.is_consumable():
                    task.inputs[local_id] = conc_ref
                    if isinstance(conc_ref, SW2_StreamReference):
                        task.unfinished_input_streams.add(ref.id)
                        self.subscribe_task_to_ref(task, conc_ref)
                else:
                    # The reference is a future that has not yet been produced,
                    # so block the task.
                    task_will_block = True
                    task.block_on(ref.id, local_id)
                    
                    # We may need to recursively check the inputs on the
                    # producing task for this reference.
                    try:
                        producing_task = self.task_for_output[ref.id]
                    except KeyError:
                        ciel.log.error('Task %s cannot access missing input %s and will block until this is produced' % (task.task_id, ref.id), 'TASKPOOL', logging.WARNING)
                        continue
                    
                    # The producing task is inactive, so recursively visit it.                    
                    if producing_task.state in (TASK_CREATED, TASK_COMMITTED):
                        producing_task.set_state(TASK_BLOCKING)
                        newly_active_task_queue.append(producing_task)
            
            # If all inputs are available, we can now run this task. Otherwise,
            # it will run when its inputs are published.
            if not task_will_block:
                task.set_state(TASK_RUNNABLE)
                should_schedule = True
                self.add_runnable_task(task)
                
        if should_schedule:
            self.bus.publish('schedule')
    
class LazyTaskPoolAdapter:
    """
    We use this adapter class to convert from the view's idea of a task pool to
    the new LazyTaskPool.
    """
    
    def __init__(self, lazy_task_pool, task_failure_investigator):
        self.lazy_task_pool = lazy_task_pool
        self.task_failure_investigator = task_failure_investigator
        
        # XXX: This exposes the task pool to the view.
        self.tasks = lazy_task_pool.tasks
     
    def add_task(self, task_descriptor, parent_task=None, job=None, may_reduce=True):
        try:
            task_id = task_descriptor['task_id']
        except:
            task_id = self.generate_task_id()
            task_descriptor['task_id'] = task_id
        
        task = build_taskpool_task_from_descriptor(task_descriptor, parent_task)
        task.job = job
        
        self.lazy_task_pool.add_task(task, parent_task is None, may_reduce)
        
        #add_event = self.new_event(task)
        #add_event["task_descriptor"] = task.as_descriptor(long=True)
        #add_event["action"] = "CREATED"
    
        #self.events.append(add_event)

        return task
    
    def get_reference_info(self, id):
        return self.lazy_task_pool.get_reference_info(id)
    
    def get_ref_by_id(self, id):
        return self.lazy_task_pool.get_ref_by_id(id)
    
    def generate_task_id(self):
        return str(uuid.uuid1())
    
    def get_task_by_id(self, id):
        return self.lazy_task_pool.get_task_by_id(id)
    
    def unsubscribe_task_from_ref(self, task, ref):
        return self.lazy_task_pool.unsubscribe_task_from_ref(task, ref)

    def publish_refs(self, task, refs):
        self.lazy_task_pool.publish_refs(refs, task.job, True)
    
    def spawn_child_tasks(self, parent_task, spawned_task_descriptors, may_reduce=True):

        if parent_task.is_replay_task():
            return
            
        for child in spawned_task_descriptors:
            try:
                spawned_task_id = child['task_id']
            except KeyError:
                raise
            
            task = self.add_task(child, parent_task, parent_task.job, may_reduce)
            parent_task.children.append(task)
            
            if task.continues_task is not None:
                parent_task.continuation = spawned_task_id

    def report_tasks(self, report):
        
        for (parent_id, success, payload) in report:
            parent_task = self.get_task_by_id(parent_id)
            if success:
                (spawned, published) = payload
                self.spawn_child_tasks(parent_task, spawned, may_reduce=False)
                self.commit_task(parent_id, {"bindings": dict([(ref.id, ref) for ref in published])}, should_publish=False)
            else:
                # Only one failed task per-report, at the moment.
                self.investigate_task_failure(parent_task, payload)
                # I hope this frees up workers and so forth?
                return
                
        toplevel_task = self.get_task_by_id(report[0][0])
        self.lazy_task_pool.do_graph_reduction(toplevel_task.expected_outputs)
        self.lazy_task_pool.worker_pool.worker_idle(toplevel_task.worker)

    def investigate_task_failure(self, task, payload):
        self.task_failure_investigator.investigate_task_failure(task, payload)

    def commit_task(self, task_id, commit_payload, should_publish=True):
        
        commit_bindings = commit_payload['bindings']
        task = self.lazy_task_pool.get_task_by_id(task_id)
        
        self.lazy_task_pool.task_completed(task, commit_bindings, should_publish)
        
        # Saved continuation URI, if necessary.
        try:
            commit_continuation_uri = commit_payload['saved_continuation_uri']
            task.saved_continuation_uri = commit_continuation_uri
        except KeyError:
            pass
