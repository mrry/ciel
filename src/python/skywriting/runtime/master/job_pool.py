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
from cherrypy.process import plugins
from shared.references import SWReferenceJSONEncoder
from skywriting.runtime.task import TASK_STATES, \
    build_taskpool_task_from_descriptor, TASK_QUEUED, TASK_FAILED,\
    TASK_COMMITTED, TASK_QUEUED_STREAMING
from threading import Lock, Condition
import Queue
import ciel
import datetime
import logging
import os
import simplejson
import struct
import time
import uuid
from skywriting.runtime.task_graph import DynamicTaskGraph, TaskGraphUpdate
from shared.references import SWErrorReference
from skywriting.runtime.master.scheduling_policy import LocalitySchedulingPolicy,\
    get_scheduling_policy
import collections

JOB_CREATED = -1
JOB_ACTIVE = 0
JOB_COMPLETED = 1
JOB_FAILED = 2
JOB_QUEUED = 3
JOB_RECOVERED = 4
JOB_CANCELLED = 5

JOB_STATES = {'CREATED': JOB_CREATED,
              'ACTIVE': JOB_ACTIVE,
              'COMPLETED': JOB_COMPLETED,
              'FAILED': JOB_FAILED,
               'QUEUED': JOB_QUEUED,
              'RECOVERED': JOB_RECOVERED,
              'CANCELLED' : JOB_CANCELLED}

JOB_STATE_NAMES = {}
for (name, number) in JOB_STATES.items():
    JOB_STATE_NAMES[number] = name

RECORD_HEADER_STRUCT = struct.Struct('!cI')

class Job:
    
    def __init__(self, id, root_task, job_dir, state, job_pool, job_options):
        self.id = id
        self.root_task = root_task
        self.job_dir = job_dir
        
        self.job_pool = job_pool
        
        self.history = []
        
        self.state = state
        
        self.runnable_queue = Queue.Queue()
        
        self.global_queues = {}
        
        self.result_ref = None

        self.task_journal_fp = None

        # Start journalling immediately to capture the root task.
        if self.task_journal_fp is None and self.job_dir is not None:
            self.task_journal_fp = open(os.path.join(self.job_dir, 'task_journal'), 'ab')

        self.job_options = job_options

        self._lock = Lock()
        self._condition = Condition(self._lock)

        # Counters for each task state.
        self.task_state_counts = {}
        for state in TASK_STATES.values():
            self.task_state_counts[state] = 0

        self.all_tasks = RunningAverage()
        self.all_tasks_by_type = {}

        try:
            self.scheduling_policy = get_scheduling_policy(self.job_options['scheduler'])
        except KeyError:
            self.scheduling_policy = LocalitySchedulingPolicy()
        
        self.task_graph = JobTaskGraph(self, self.runnable_queue)
        
        self.workers = {}
        self.job_pool.worker_pool.notify_job_about_current_workers(self)
        
    def schedule(self):
        self.job_pool.deferred_worker.do_deferred(lambda: self._schedule())
        
    def _schedule(self):
        
        ciel.log('Beginning to schedule job %s' % self.id, 'JOB', logging.INFO)
        
        with self._lock:
            
            # 1. Assign runnable tasks to worker queues.
            while True:
                try:
                    task = self.runnable_queue.get_nowait()
                    self.assign_scheduling_class_to_task(task)
                    worker = self.select_worker_for_task(task)
                    self.workers[worker].queue_task(task)
                    if task.get_constrained_location() is None:
                        self.push_task_on_global_queue(task)
                except Queue.Empty:
                    break
            
            # 2. For each worker, check if we need to assign any tasks.
            total_assigned = 0
            undersubscribed_worker_classes = []
            for worker, wstate in self.workers.items():
                for scheduling_class, capacity in worker.scheduling_classes.items():
                    num_assigned = wstate.tasks_assigned_in_class(scheduling_class)
                    while num_assigned < capacity:
                        task = wstate.pop_task_from_queue(scheduling_class)
                        if task is None:
                            break
                        elif task.state not in (TASK_QUEUED, TASK_QUEUED_STREAMING):
                            continue
                        task.add_worker(worker)
                        wstate.assign_task(task)
                        self.job_pool.worker_pool.execute_task_on_worker(worker, task)
                        num_assigned += 1
                        total_assigned += 1
                    if num_assigned < capacity:
                        undersubscribed_worker_classes.append((worker, scheduling_class, capacity - num_assigned))

            for worker, scheduling_class, deficit in undersubscribed_worker_classes:
                num_global_assigned = 0
                while num_global_assigned < deficit:
                    task = self.pop_task_from_global_queue(scheduling_class)
                    if task is None:
                        break
                    elif task.state not in (TASK_QUEUED, TASK_QUEUED_STREAMING):
                        continue
                    task.add_worker(worker)
                    self.workers[worker].assign_task(task)
                    self.job_pool.worker_pool.execute_task_on_worker(worker, task)
                    num_global_assigned += 1
        
        ciel.log('Finished scheduling job %s. Tasks assigned = %d' % (self.id, total_assigned), 'JOB', logging.INFO)
        
    def pop_task_from_global_queue(self, scheduling_class):
        if scheduling_class == '*':
            for queue in self.global_queues.values():
                try:
                    return queue.popleft()
                except IndexError:
                    pass
            return None
        else:
            try:
                return self.global_queues[scheduling_class].popleft()
            except IndexError:
                return None
            except KeyError:
                return None
        
    def push_task_on_global_queue(self, task):
        try:
            class_queue = self.global_queues[task.scheduling_class]
        except KeyError:
            class_queue = collections.deque()
            self.global_queues[task.scheduling_class] = class_queue
        class_queue.append(task)
        
    def select_worker_for_task(self, task):
        constrained_location = task.get_constrained_location()
        if constrained_location is not None:
            worker = self.job_pool.worker_pool.get_worker_at_netloc(constrained_location)
        elif task.state in (TASK_QUEUED_STREAMING, TASK_QUEUED):
            worker, _ = self.scheduling_policy.select_worker_for_task(task, self.job_pool.worker_pool)
        else:
            ciel.log.error("Task %s scheduled in bad state %s; ignored" % (task, task.state), 
                               "SCHEDULER", logging.ERROR)
            raise
        return worker
        
    def assign_scheduling_class_to_task(self, task):
        if task.scheduling_class is not None:
            return
        elif task.handler == 'swi':
            task.scheduling_class = 'cpu'
        elif task.handler == 'init':
            task.scheduling_class = 'cpu'
        elif task.handler == 'sync':
            task.scheduling_class = 'cpu'
        elif task.handler == 'grab':
            task.scheduling_class = 'disk'
        elif task.handler == 'java':
            task.scheduling_class = 'disk'
        else:
            task.scheduling_class = 'disk'

    def record_event(self, description):
        self.history.append((datetime.datetime.now(), description))
                    
    def set_state(self, state):
        self.record_event(JOB_STATE_NAMES[state])
        self.state = state
        evt_time = self.history[-1][0]
        ciel.log('%s %s @ %f' % (self.id, JOB_STATE_NAMES[self.state], time.mktime(evt_time.timetuple()) + evt_time.microsecond / 1e6), 'JOB', logging.INFO)

    def failed(self):
        # Done under self._lock (via _report_tasks()).
        self.set_state(JOB_FAILED)
        self.stop_journalling()
        self._condition.notify_all()

    def enqueued(self):
        self.set_state(JOB_QUEUED)

    def completed(self, result_ref):
        # Done under self._lock (via _report_tasks()).
        self.set_state(JOB_COMPLETED)
        self.result_ref = result_ref
        self._condition.notify_all()
        self.stop_journalling()
        self.job_pool.job_completed(self)

    def activated(self):
        self.set_state(JOB_ACTIVE)
        mjo = MasterJobOutput(self.root_task.expected_outputs, self)
        for output in self.root_task.expected_outputs:
            self.task_graph.subscribe(output, mjo)
        self.task_graph.reduce_graph_for_references(self.root_task.expected_outputs)
        self.schedule()

    def cancelled(self):
        self.set_state(JOB_CANCELLED)
        self.stop_journalling()

    def stop_journalling(self):
        # Done under self._lock (via _report_tasks()).        
        if self.task_journal_fp is not None:
            self.task_journal_fp.close()
        self.task_journal_fp = None
                
        if self.job_dir is not None:
            with open(os.path.join(self.job_dir, 'result'), 'w') as result_file:
                simplejson.dump(self.result_ref, result_file, cls=SWReferenceJSONEncoder)

    def flush_journal(self):
        with self._lock:
            if self.task_journal_fp is not None:
                self.task_journal_fp.flush()
                os.fsync(self.task_journal_fp.fileno())

    def add_reference(self, id, ref, should_sync=False):
        # Called under self._lock (from _report_tasks()).
        if self.task_journal_fp is not None:
            ref_details = simplejson.dumps({'id': id, 'ref': ref}, cls=SWReferenceJSONEncoder)
            self.task_journal_fp.write(RECORD_HEADER_STRUCT.pack('R', len(ref_details)))
            self.task_journal_fp.write(ref_details)
            if should_sync:
                self.task_journal_fp.flush()
                os.fsync(self.task_journal_fp.fileno())

    def add_task(self, task, should_sync=False):
        # Called under self._lock (from _report_tasks()).
        self.task_state_counts[task.state] = self.task_state_counts[task.state] + 1
        if self.task_journal_fp is not None:
            task_details = simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder)
            self.task_journal_fp.write(RECORD_HEADER_STRUCT.pack('T', len(task_details)))
            self.task_journal_fp.write(task_details)
            if should_sync:
                self.task_journal_fp.flush()
                os.fsync(self.task_journal_fp.fileno())

#    def steal_task(self, worker, scheduling_class):
#        ciel.log('In steal_task(%s, %s)' % (worker.id, scheduling_class), 'LOG', logging.INFO)
#        # Stealing policy: prefer task with fewest replicas, then lowest cost on this worker.
#        best_candidate = (sys.maxint, 0, None)
#        for victim in self.workers.values():
#            if victim.worker == worker:
#                continue
#            task = victim.get_last_task_in_class(scheduling_class)
#            if task is None:
#                continue
#            num_workers = len(task.get_workers())
#            cost = self.guess_task_cost_on_worker(task, worker)
#            best_candidate = min(best_candidate, (num_workers, cost, task))
#        
#        task = best_candidate[2]
#        if task is not None:
#            task.add_worker(worker)
#            self.workers[worker].add_task(task)
#            self.job_pool.worker_pool.execute_task_on_worker(worker, task)
            
    def record_state_change(self, prev_state, next_state):
        # Done under self._lock (from _report_tasks()).
        self.task_state_counts[prev_state] = self.task_state_counts[prev_state] - 1
        self.task_state_counts[next_state] = self.task_state_counts[next_state] + 1

    def as_descriptor(self):
        counts = {}
        ret = {'job_id': self.id, 
               'task_counts': counts, 
               'state': JOB_STATE_NAMES[self.state], 
               'root_task': self.root_task.task_id if self.root_task is not None else None,
               'expected_outputs': self.root_task.expected_outputs if self.root_task is not None else None,
               'result_ref': self.result_ref,
               'job_options' : self.job_options}
        with self._lock:
            for (name, state_index) in TASK_STATES.items():
                counts[name] = self.task_state_counts[state_index]
        return ret

    def report_tasks(self, report, toplevel_task, worker):
        self.job_pool.deferred_worker.do_deferred(lambda: self._report_tasks(report, toplevel_task, worker))

    def _report_tasks(self, report, toplevel_task, worker):
        with self._lock:
    
            tx = TaskGraphUpdate()
            
            root_task = self.task_graph.get_task(report[0][0])
            for assigned_worker in root_task.get_workers():
                if assigned_worker is worker:
                    self.workers[worker].deassign_task(root_task)
                else:
                    self.workers[assigned_worker].deassign_task(root_task)
                    assigned_worker.worker_pool.abort_task_on_worker(root_task, assigned_worker)
                    
                    # XXX: Need to abort the task running on other workers.
                    pass
            
            for (parent_id, success, payload) in report:
                
                parent_task = self.task_graph.get_task(parent_id)
                
                if success:
                    (spawned, published, profiling) = payload
                    parent_task.set_profiling(profiling)
                    parent_task.set_state(TASK_COMMITTED)
                    self.record_task_stats(parent_task, worker)
                    for child in spawned:
                        child_task = build_taskpool_task_from_descriptor(child, parent_task)
                        tx.spawn(child_task)
                        parent_task.children.append(child_task)
                    
                    for ref in published:
                        tx.publish(ref, parent_task)
                
                else:
                    # Only one failed task per-report, at the moment.
                    self.investigate_task_failure(parent_task, payload)
                    self.schedule()
                    return
                    
            tx.commit(self.task_graph)
            self.task_graph.reduce_graph_for_references(toplevel_task.expected_outputs)
            
        # XXX: Need to remove assigned task from worker(s).
        self.schedule()

    def record_task_stats(self, task, worker):
        try:
            task_profiling = task.get_profiling()
            task_type = task.get_type()
            task_execution_time = task_profiling['FINISHED'] - task_profiling['STARTED']
            
            self.all_tasks.update(task_execution_time)
            try:
                self.all_tasks_by_type[task_type].update(task_execution_time)
            except KeyError:
                self.all_tasks_by_type[task_type] = RunningAverage(task_execution_time)
                
            self.workers[worker].record_task_stats(task)

        except:
            ciel.log('Error recording task statistics for task: %s' % task.task_id, 'JOB', logging.WARNING)

    def guess_task_cost_on_worker(self, task, worker):
        return self.workers[worker].load(task.scheduling_class, True)
                
    def investigate_task_failure(self, task, payload):
        self.job_pool.task_failure_investigator.investigate_task_failure(task, payload)
        
    def notify_worker_added(self, worker):
        with self._lock:
            try:
                _ = self.workers[worker] 
                return
            except KeyError:
                ciel.log('Job %s notified that worker being added' % self.id, 'JOB', logging.INFO)
                worker_state = JobWorkerState(worker)
                self.workers[worker] = worker_state
        self.schedule()
    
    def notify_worker_failed(self, worker):
        with self._lock:
            try:
                worker_state = self.workers[worker]
                del self.workers[worker]
                ciel.log('Reassigning tasks from failed worker %s for job %s' % (worker.id, self.id), 'JOB', logging.WARNING)
                for assigned in worker_state.assigned_tasks.values():
                    for failed_task in assigned:
                        failed_task.remove_worker(worker)
                        self.investigate_task_failure(failed_task, ('WORKER_FAILED', None, {}))
                for scheduling_class in worker_state.queues:
                    while True:
                        queued_task = worker_state.pop_task_from_queue(scheduling_class)
                        if queued_task is None:
                            break
                        self.runnable_queue.put(queued_task)
                        #self.investigate_task_failure(failed_task, ('WORKER_FAILED', None, {}))
                        #self.runnable_queue.put(queued_task)
                self.schedule()
            except KeyError:
                ciel.log('Weird keyerror coming out of notify_worker_failed', 'JOB', logging.WARNING, True)
                pass

class JobWorkerState:
    
    def __init__(self, worker):
        self.worker = worker
        self.assigned_tasks = {}
        self.queues = {}
        self.running_average = RunningAverage()
        self.running_average_by_type = {}
        
#    def get_last_task_in_class(self, scheduling_class):
#        ciel.log('In get_last_task_in_class(%s, %s)' % (self.worker.id, scheduling_class), 'JWS', logging.INFO)
#        eff_class = self.worker.get_effective_scheduling_class(scheduling_class)
#        try:
#            ciel.log('Returning task: %s' % (repr(self.assigned_tasks[eff_class][-1])), 'JWS', logging.INFO)
#            return self.assigned_tasks[eff_class][-1]
#        except:
#            # IndexError or KeyError is valid here.
#            return None
        
    def tasks_assigned_in_class(self, scheduling_class):
        eff_class = self.worker.get_effective_scheduling_class(scheduling_class)
        try:
            return len(self.assigned_tasks[eff_class])
        except KeyError:
            return 0
        
    def pop_task_from_queue(self, scheduling_class):
        eff_class = self.worker.get_effective_scheduling_class(scheduling_class)
        try:
            task = self.queues[eff_class].popleft()
            return task
        except KeyError:
            return None
        except IndexError:
            return None
        
    def queue_task(self, task):
        eff_class = self.worker.get_effective_scheduling_class(task.scheduling_class)
        try:
            self.queues[eff_class].append(task)
        except KeyError:
            class_queue = collections.deque()
            self.queues[eff_class] = class_queue
            class_queue.append(task)
        
    def assign_task(self, task):
        eff_class = self.worker.get_effective_scheduling_class(task.scheduling_class)
        try:
            self.assigned_tasks[eff_class].append(task)
        except KeyError:
            class_queue = collections.deque()
            self.assigned_tasks[eff_class] = class_queue
            class_queue.append(task)
        
    def deassign_task(self, task):
        try:
            eff_class = self.worker.get_effective_scheduling_class(task.scheduling_class)
            self.assigned_tasks[eff_class].remove(task)
        except KeyError:
            # XXX: This is happening twice, once on receiving the report and again on the failure.
            pass
        
    def load(self, scheduling_class, normalized=False):
        eff_class = self.worker.get_effective_scheduling_class(scheduling_class)
        norm = float(self.worker.get_effective_scheduling_class_capacity(eff_class)) if normalized else 1.0
        ret = 0.0
        try:
            ret += len(self.queues[eff_class]) / norm
            ret += len(self.assigned_tasks[eff_class]) / norm
            return ret
        except KeyError:
            pass
        except:
            ciel.log('Weird exception in jws.load()', 'JWS', logging.ERROR, True)
        return ret

    
    def record_task_stats(self, task):
        try:
            task_profiling = task.get_profiling()
            task_type = task.get_type()
            task_execution_time = task_profiling['FINISHED'] - task_profiling['STARTED']
            
            self.running_average.update(task_execution_time)
            try:
                self.running_average_by_type[task_type].update(task_execution_time)
            except KeyError:
                self.running_average_by_type[task_type] = RunningAverage(task_execution_time)

        except:
            ciel.log('Error recording task statistics for task: %s' % task.task_id, 'JOB', logging.WARNING)


class RunningAverage:
    
    NEGATIVE_INF = float('-Inf')
    POSITIVE_INF = float('+Inf')
    
    def __init__(self, initial_observation=None):
        if initial_observation is None:
            self.min = RunningAverage.POSITIVE_INF
            self.max = RunningAverage.NEGATIVE_INF
            self.total = 0.0
            self.count = 0
        else:
            self.min = initial_observation
            self.max = initial_observation
            self.total = initial_observation
            self.count = 1
        
    def update(self, observation):
        self.total += observation
        self.count += 1
        self.max = max(self.max, observation)
        self.min = min(self.min, observation)
        
    def get(self):
        if self.count > 0:
            return self.total / self.count
        else:
            return float('+NaN')

class MasterJobOutput:
    
    def __init__(self, required_ids, job):
        self.required_ids = set(required_ids)
        self.job = job
    def is_queued_streaming(self):
        return False
    def is_blocked(self):
        return True
    def is_complete(self):
        return len(self.required_ids) == 0
    def notify_ref_table_updated(self, ref_table_entry):
        self.required_ids.discard(ref_table_entry.ref.id)
        if self.is_complete():
            self.job.completed(ref_table_entry.ref)

class JobTaskGraph(DynamicTaskGraph):
    
    
    def __init__(self, job, scheduler_queue):
        DynamicTaskGraph.__init__(self)
        self.job = job
        self.scheduler_queue = scheduler_queue
    
    def spawn(self, task, tx=None):
        self.job.add_task(task)
        DynamicTaskGraph.spawn(self, task, tx)
        
    def publish(self, reference, producing_task=None):
        self.job.add_reference(reference.id, reference)
        return DynamicTaskGraph.publish(self, reference, producing_task)
    
    def task_runnable(self, task):
        if self.job.state == JOB_ACTIVE:
            task.set_state(TASK_QUEUED)
            self.scheduler_queue.put(task)
        else:
            ciel.log('Task %s became runnable while job %s not active (%s): ignoring' % (task.task_id, self.job.id, JOB_STATE_NAMES[self.job.state]), 'JOBTASKGRAPH', logging.WARN)

    def task_failed(self, task, bindings, reason, details=None):

        ciel.log.error('Task failed because %s' % (reason, ), 'TASKPOOL', logging.WARNING)
        should_notify_outputs = False

        task.record_event(reason)

        for ref in bindings.values():
            self.publish(ref, None)

        if reason == 'WORKER_FAILED':
            # Try to reschedule task.
            task.current_attempt += 1
            # XXX: Remove this hard-coded constant. We limit the number of
            #      retries in case the task is *causing* the failures.
            if task.current_attempt > 3:
                task.set_state(TASK_FAILED)
                should_notify_outputs = True
            else:
                ciel.log.error('Rescheduling task %s after worker failure' % task.task_id, 'TASKFAIL', logging.WARNING)
                task.set_state(TASK_FAILED)
                self.task_runnable(task)
                
        elif reason == 'MISSING_INPUT':
            # Problem fetching input, so we will have to re-execute it.
            for binding in bindings.values():
                ciel.log('Missing input: %s' % str(binding), 'TASKFAIL', logging.WARNING)
            self.handle_missing_input(task)
            
        elif reason == 'RUNTIME_EXCEPTION':
            # A hard error, so kill the entire job, citing the problem.
            task.set_state(TASK_FAILED)
            should_notify_outputs = True

        if should_notify_outputs:
            for output in task.expected_outputs:
                ciel.log('Publishing error reference for %s (because %s)' % (output, reason), 'TASKFAIL', logging.ERROR)
                self.publish(SWErrorReference(output, reason, details), task)
                
        self.job.schedule()

    def handle_missing_input(self, task):
        task.set_state(TASK_FAILED)

        # Assume that all of the dependencies are unavailable.
        task.convert_dependencies_to_futures()
        
        # We will re-reduce the graph for this task, ignoring the network
        # locations for which getting the input failed.
        # N.B. We should already have published the necessary tombstone refs
        #      for the failed inputs.
        self.reduce_graph_for_tasks([task])

class JobPool(plugins.SimplePlugin):

    def __init__(self, bus, journal_root, scheduler, task_failure_investigator, deferred_worker, worker_pool):
        plugins.SimplePlugin.__init__(self, bus)
        self.journal_root = journal_root
        
        self.scheduler = scheduler
        self.task_failure_investigator = task_failure_investigator
        self.deferred_worker = deferred_worker
        self.worker_pool = worker_pool
    
        # Mapping from job ID to job object.
        self.jobs = {}
        
        
        self.current_running_job = None
        self.run_queue = Queue.Queue()
        
        self.num_running_jobs = 0
        self.max_running_jobs = 10
        
        # Synchronisation code for stopping/waiters.
        self.is_stopping = False
        self.current_waiters = 0
        self.max_concurrent_waiters = 10
        
        self._lock = Lock()
    
    def subscribe(self):
        # Higher priority than the HTTP server
        self.bus.subscribe("stop", self.server_stopping, 10)

    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.server_stopping)
        
    def start_all_jobs(self):
        for job in self.jobs.values():
            self.queue_job(job)
        
    def server_stopping(self):
        # When the server is shutting down, we need to notify all threads
        # waiting on job completion.
        self.is_stopping = True
        for job in self.jobs.values():
            with job._lock:
                job._condition.notify_all()
        
    def get_job_by_id(self, id):
        return self.jobs[id]
    
    def get_all_job_ids(self):
        return self.jobs.keys()
    
    def allocate_job_id(self):
        return str(uuid.uuid1())
    
    def add_job(self, job, sync_journal=False):
        self.jobs[job.id] = job
        
        # We will use this both for new jobs and on recovery.
        if job.root_task is not None:
            job.task_graph.spawn(job.root_task)
    
    def notify_worker_added(self, worker):
        for job in self.jobs.values():
            if job.state == JOB_ACTIVE:
                job.notify_worker_added(worker)
            
    def notify_worker_failed(self, worker):
        for job in self.jobs.values():
            if job.state == JOB_ACTIVE:
                job.notify_worker_failed(worker)
                
    def add_failed_job(self, job_id):
        # XXX: We lose job options... should probably persist these in the journal.
        job = Job(job_id, None, None, JOB_FAILED, self, {})
        self.jobs[job_id] = job
    
    def create_job_for_task(self, task_descriptor, job_options, job_id=None):
        
        with self._lock:
        
            if job_id is None:
                job_id = self.allocate_job_id()
            task_id = 'root:%s' % (job_id, )
    
            task_descriptor['task_id'] = task_id
    
            # TODO: Here is where we will set up the job journal, etc.
            job_dir = self.make_job_directory(job_id)
            
            try:
                expected_outputs = task_descriptor['expected_outputs']
            except KeyError:
                expected_outputs = ['%s:job_output' % job_id]
                task_descriptor['expected_outputs'] = expected_outputs
                
            task = build_taskpool_task_from_descriptor(task_descriptor, None)
            job = Job(job_id, task, job_dir, JOB_CREATED, self, job_options)
            task.job = job
            
            self.add_job(job)
            
            ciel.log('Added job: %s' % job.id, 'JOB_POOL', logging.INFO)
    
            return job

    def make_job_directory(self, job_id):
        if self.journal_root is not None:
            job_dir = os.path.join(self.journal_root, job_id)
            os.mkdir(job_dir)
            return job_dir
        else:
            return None

    def maybe_start_new_job(self):
        with self._lock:
            if self.num_running_jobs < self.max_running_jobs:
                try:
                    next_job = self.run_queue.get_nowait()
                    self.num_running_jobs += 1
                    self._start_job(next_job)
                except Queue.Empty:
                    ciel.log('Not starting a new job because there are no more to start', 'JOB_POOL', logging.INFO)
            else:
                ciel.log('Not starting a new job because there is insufficient capacity', 'JOB_POOL', logging.INFO)
                
    def queue_job(self, job):
        self.run_queue.put(job)
        job.enqueued()
        self.maybe_start_new_job()
            
    def job_completed(self, job):
        self.num_running_jobs -= 1
        self.maybe_start_new_job()

    def _start_job(self, job):
        ciel.log('Starting job ID: %s' % job.id, 'JOB_POOL', logging.INFO)
        # This will also start the job by subscribing to the root task output and reducing.
        job.activated()

    def wait_for_completion(self, job):
        with job._lock:
            ciel.log('Waiting for completion of job %s' % job.id, 'JOB_POOL', logging.INFO)
            while job.state not in (JOB_COMPLETED, JOB_FAILED):
                if self.is_stopping:
                    break
                elif self.current_waiters > self.max_concurrent_waiters:
                    break
                else:
                    self.current_waiters += 1
                    job._condition.wait()
                    self.current_waiters -= 1
            if self.is_stopping:
                raise Exception("Server stopping")
            elif self.current_waiters >= self.max_concurrent_waiters:
                raise Exception("Too many concurrent waiters")
            else:
                return job
