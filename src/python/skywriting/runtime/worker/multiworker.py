# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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
from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    AbortedException
from skywriting.runtime.local_task_graph import LocalTaskGraph, LocalJobOutput
from skywriting.runtime.task_executor import TaskExecutionRecord
import Queue
import ciel
import logging
import random
import threading
import math

EWMA_ALPHA = 0.75
INITIAL_TASK_COST = 0.5

class WorkerJob:
    
    def __init__(self, id, worker, tickets=1000000):
        self.id = id
        self.incoming_queues = {}
        self.runnable_queues = {}
        
        for scheduling_class in worker.scheduling_classes.keys():
            self.incoming_queues[scheduling_class] = Queue.Queue()
            self.runnable_queues[scheduling_class] = Queue.Queue()
        
        self.reference_cache = {}
        self.task_graph = LocalTaskGraph(worker.execution_features, runnable_queues=self.runnable_queues)
        self.active_or_queued_tasksets = 0
        self.running_tasks = 0
        self.active_tasksets = {}
        self.tickets = tickets
        self.job_aborted = False
        self._tasksets_lock = threading.Lock()
        self.task_cost = INITIAL_TASK_COST

    def add_taskset(self, taskset):
        with self._tasksets_lock:
            if not self.job_aborted: 
                try:
                    self.incoming_queues[taskset.initial_td['scheduling_class']].put(taskset)
                except KeyError:
                    try:
                        self.incoming_queues['*'].put(taskset)
                    except KeyError:
                        ciel.log('Scheduling class %s not supported on this worker (for taskset %s)' % (taskset.initial_td['scheduling_class'], taskset.initial_td['task_id']), 'WJOB', logging.ERROR)
                        raise
                self.active_or_queued_tasksets += 1
            else:
                raise AbortedException()
        
    def abort_all_active_tasksets(self, id):
        with self._tasksets_lock:
            self.job_aborted = True
            for taskset in self.active_tasksets.values():
                taskset.abort_all_tasks()
            
    def abort_taskset_with_id(self, id):
        try:
            taskset = self.active_tasksets[id]
            taskset.abort_all_tasks()
            # Taskset completion routine will eventually call self.taskset_completed(taskset)
        except KeyError:
            pass
        
    def get_tickets(self):
        return math.ceil(self.tickets * (INITIAL_TASK_COST / self.task_cost))
        
    def taskset_activated(self, taskset):
        with self._tasksets_lock:
            if not self.job_aborted:
                self.active_tasksets[taskset.id] = taskset
            else:
                raise AbortedException()
        
    def taskset_completed(self, taskset):
        with self._tasksets_lock:
            del self.active_tasksets[taskset.id]
            self.active_or_queued_tasksets -= 1
            
    def task_started(self):
        self.running_tasks += 1
        ciel.log('Job %s started a task (now running %d)' % (self.id, self.running_tasks), 'JOB', logging.INFO)
        
    def task_finished(self, task, time):
        self.running_tasks -= 1
        self.task_cost = EWMA_ALPHA * time + (1 - EWMA_ALPHA) * self.task_cost
        ciel.log('Job %s finished a task (now running %d, task cost now %f)' % (self.id, self.running_tasks, self.task_cost), 'JOB', logging.INFO)

class MultiWorker:
    """FKA JobManager."""
    
    def __init__(self, bus, worker):
        self.worker = worker
        self.jobs = {}
        self.scheduling_classes = worker.scheduling_classes
        self._lock = threading.Lock()
        self.queue_manager = QueueManager(bus, self, worker)
        
        self.thread_pools = {}
        for (scheduling_class, capacity) in worker.scheduling_classes.items():
            self.thread_pools[scheduling_class] = WorkerThreadPool(bus, self.queue_manager, scheduling_class, capacity)
        
    def subscribe(self):
        self.queue_manager.subscribe()
        for thread_pool in self.thread_pools.values():
            thread_pool.subscribe()
    
    def unsubscribe(self):
        self.queue_manager.unsubscribe()
        for thread_pool in self.thread_pools.values():
            thread_pool.unsubscribe()
    
    def num_active_jobs(self):
        return len(self.jobs)
    
    def get_active_jobs(self):
        return self.jobs.values()
    
    def get_job_by_id(self, job_id):
        return self.jobs[job_id]
    
    def create_and_queue_taskset(self, task_descriptor):
        with self._lock:
            job_id = task_descriptor['job']
            try:
                job = self.jobs[job_id]
            except:
                job = WorkerJob(job_id, self.worker)
                self.jobs[job_id] = job
                
            taskset = MultiWorkerTaskSetExecutionRecord(task_descriptor, self.worker.block_store, self.worker.master_proxy, self.worker.execution_features, self.worker, job, self)
            job.add_taskset(taskset)

        # XXX: Don't want to do this immediately: instead block until the runqueue gets below a certain length.
        taskset.start()

    def taskset_completed(self, taskset):
        with self._lock:
            taskset.job.taskset_completed(taskset)
            if taskset.job.active_or_queued_tasksets == 0:
                del self.jobs[taskset.job.id]

class MultiWorkerTaskSetExecutionRecord:

    def __init__(self, root_task_descriptor, block_store, master_proxy, execution_features, worker, job, job_manager):
        self.id = root_task_descriptor['task_id']
        self._record_list_lock = threading.Lock()
        self.task_records = []
        self.block_store = worker.block_store
        self.master_proxy = worker.master_proxy
        self.execution_features = worker.execution_features
        self.worker = worker
        self.reference_cache = job.reference_cache
        # XXX: Should possibly combine_with()?
        for ref in root_task_descriptor['inputs']:
            self.reference_cache[ref.id] = ref
        self.initial_td = root_task_descriptor
        self.task_graph = job.task_graph
        
        self._refcount = 0
        
        self.job = job
        self.job_manager = job_manager
        
        self.aborted = False
        
        # LocalJobOutput gets self so that it can notify us when done.
        self.job_output = LocalJobOutput(self.initial_td["expected_outputs"], self)

    def abort_all_tasks(self):
        # This will inhibit the sending of a report, and also the creation of any new task records.
        self.aborted = True

        with self._record_list_lock:
            for record in self.task_records:
                record.abort()
            
    def inc_runnable_count(self):
        self._refcount += 1
        
    def dec_runnable_count(self):
        self._refcount -= 1
        # Note that we only notify when the count comes back down to zero.
        if self._refcount == 0:
            self.notify_completed()

    def start(self):
        ciel.log.error('Starting taskset with %s' % self.initial_td['task_id'], 'TASKEXEC', logging.INFO)
        self.job.taskset_activated(self)
        
        self.task_graph.add_root_task_id(self.initial_td['task_id'])
        for ref in self.initial_td['expected_outputs']:
            self.task_graph.subscribe(ref, self.job_output)
        # This pokes the root task into the job's runnable_queue.
        self.task_graph.spawn_and_publish([self.initial_td], self.initial_td["inputs"], taskset=self)
        
        # Notify a sleeping worker thread.
        self.job_manager.queue_manager.notify(self.initial_td['scheduling_class'])

    def notify_completed(self):
        """Called by LocalJobOutput.notify_ref_table_updated() when the taskset is complete."""
        ciel.log.error('Taskset complete', 'TASKEXEC', logging.INFO)
        
        if not self.aborted:
            # Send a task report back to the master.
            report_data = []
            for tr in self.task_records:
                if tr.success:
                    report_data.append((tr.task_descriptor['task_id'], tr.success, (tr.spawned_tasks, tr.published_refs, tr.get_profiling())))
                else:
                    ciel.log('Appending failure to report for task %s' % tr.task_descriptor['task_id'], 'TASKEXEC', logging.INFO)
                    report_data.append((tr.task_descriptor['task_id'], tr.success, (tr.failure_reason, tr.failure_details, tr.failure_bindings)))
            self.master_proxy.report_tasks(self.job.id, self.initial_td['task_id'], report_data)

        # Release this task set, which may allow the JobManager to delete the job.
        self.job_manager.taskset_completed(self)

    def build_task_record(self, task_descriptor):
        """Creates a new TaskExecutionRecord for the given task, and adds it to the journal for this task set."""
        with self._record_list_lock:
            if not self.aborted:
                record = TaskExecutionRecord(task_descriptor, self, self.execution_features, self.block_store, self.master_proxy, self.worker)
                self.task_records.append(record) 
                return record
            else:
                raise AbortedException()

    def retrieve_ref(self, ref):
        if ref.is_consumable():
            return ref
        else:
            try:
                return self.reference_cache[ref.id]
            except KeyError:
                raise ReferenceUnavailableException(ref.id)

    def publish_ref(self, ref):
        self.reference_cache[ref.id] = ref

class QueueManager:
    
    def __init__(self, bus, job_manager, worker):
        self.bus = bus
        self.job_manager = job_manager
        self.worker = worker
        #self._lock = threading.Lock()
        self._cond = {}
        
        self.current_heads = {}
        for scheduling_class in worker.scheduling_classes.keys():
            self.current_heads[scheduling_class] = {}
            self._cond[scheduling_class] = threading.Condition()
            
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop, 25)
            
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)

    def start(self):
        self.is_running = True

    def stop(self):
        self.is_running = False
        for cond in self._cond.values():
            with cond:
                cond.notifyAll()
            
    def notify(self, scheduling_class):
        try:
            with self._cond[scheduling_class]:
                ciel.log('Notifying Qmanager for class %s' % scheduling_class, 'LOTTERY', logging.INFO)
                self._cond[scheduling_class].notify()
        except KeyError:
            try:
                with self._cond['*']:
                    ciel.log('Notifying Qmanager for class *', 'LOTTERY', logging.INFO)
                    self._cond['*'].notify()
            except:
                assert False
            
    def get_next_task(self, scheduling_class):
        
        current_heads = self.current_heads[scheduling_class]
        
        with self._cond[scheduling_class]:
            
            # Loop until a task has been assigned, or we get terminated. 
            while self.is_running:
                ticket_list = []
                total_tickets = 0
                for job in self.job_manager.get_active_jobs():
                    try:
                        candidate = current_heads[job]
                    except KeyError:
                        try:
                            candidate = job.runnable_queues[scheduling_class].get_nowait()
                            current_heads[job] = candidate
                        except Queue.Empty:
                            continue
                        
                    job_tickets = job.get_tickets()
                    total_tickets += job_tickets
                    ticket_list.append((job, job_tickets, candidate))
        
                ciel.log('Total tickets in all runnable jobs is %d' % total_tickets, 'LOTTERY', logging.INFO)
                
                if total_tickets > 0:
                    chosen_ticket = random.randrange(total_tickets)
                    ciel.log('Chose ticket: %d' % chosen_ticket, 'LOTTERY', logging.INFO)
                    
                    curr_ticket = 0
                    for job, job_tickets, current_head in ticket_list:
                        curr_ticket += job_tickets
                        if curr_ticket > chosen_ticket:
                            ciel.log('Ticket corresponds to job: %s' % job.id, 'LOTTERY', logging.INFO)
                            # Choose the current head from this job.
                            del current_heads[job]
                            return current_head

                self._cond[scheduling_class].wait()
                
        # If we return None, the consuming thread should terminate.
        return None
            
class WorkerThreadPool:
    
    def __init__(self, bus, queue_manager, scheduling_class, num_threads=1):
        self.bus = bus
        self.queue_manager = queue_manager
        self.scheduling_class = scheduling_class
        self.num_threads = num_threads
        self.is_running = False
        self.threads = []
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        # Must run after QueueManager.stop()
        self.bus.subscribe('stop', self.stop ,50)
            
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
            
    def start(self):
        self.is_running = True
        for _ in range(self.num_threads):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
                
    def stop(self):
        self.is_running = False
        for thread in self.threads:
            thread.join()
        self.threads = []
        
    def thread_main(self):
        while True:
            task = self.queue_manager.get_next_task(self.scheduling_class)
            if task is None:
                return
            else:
                try:
                    self.handle_task(task)
                except Exception:
                    ciel.log.error('Uncaught error handling task in pool: %s' % (self.scheduling_class), 'MULTIWORKER', logging.ERROR, True)
                self.queue_manager.notify(self.scheduling_class)

    def handle_task(self, task):
        next_td = task.as_descriptor()
        next_td["inputs"] = [task.taskset.retrieve_ref(ref) for ref in next_td["dependencies"]]
        try:
            task_record = task.taskset.build_task_record(next_td)
            task_record.task_set.job.task_started()
            try:
                task_record.run()
            except:
                ciel.log.error('Error during executor task execution', 'MWPOOL', logging.ERROR, True)
            execution_time = task_record.finish_time - task_record.start_time
            execution_secs = execution_time.seconds + execution_time.microseconds / 1000000.0
            task_record.task_set.job.task_finished(task, execution_secs)
            if task_record.success:
                task.taskset.task_graph.spawn_and_publish(task_record.spawned_tasks, task_record.published_refs, next_td)
        except AbortedException:
            ciel.log('Task %s was aborted, skipping' % task.task_id, 'MWPOOL', logging.INFO)
        task.taskset.dec_runnable_count()