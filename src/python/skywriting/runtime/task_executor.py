# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#                    Christopher Smowton <chris.smowton@cl.cam.ac.uk>
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
from __future__ import with_statement
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from skywriting.runtime.exceptions import ReferenceUnavailableException
from skywriting.runtime.worker.local_task_graph import LocalTaskGraph, LocalJobOutput
from threading import Lock
import logging
import hashlib
import ciel

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, block_store, master_proxy, execution_features, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
        self.block_store = block_store
        self.master_proxy = master_proxy
        self.execution_features = execution_features

        self.executor_cache = ExecutorCache(self.execution_features, self.block_store)
        self.current_task_set = None
        self._lock = Lock()
    
    # Out-of-thread asynchronous notification calls

    def abort_task(self, task_id):
        with self._lock:
            if self.current_task_set is not None:
                self.current_task_set.abort_task(task_id)

    def notify_streams_done(self, task_id):
        with self._lock:
            if self.current_task_set is not None:
                self.current_task_set.notify_streams_done(task_id)
    
    # Main entry point

    def handle_input(self, input):

        new_task_set = TaskSetExecutionRecord(self.executor_cache, input, self.block_store, self.master_proxy)
        with self._lock:
            self.current_task_set = new_task_set
        new_task_set.run()
        report_data = [(tr.task_descriptor["task_id"], tr.spawned_tasks, tr.published_refs) for tr in new_task_set.task_records]
        self.master_proxy.report_tasks(report_data)
        with self._lock:
            self.current_task_set = None

class ExecutorCache:

    # A cache of executors, permitting helper processes to outlive tasks.
    # Policy: only keep one of each kind around at any time.

    def __init__(self, execution_features, block_store):
        self.execution_features = execution_features
        self.block_store = block_store
        self.idle_executors = dict()
    
    def get_executor(self, handler):
        try:
            return self.idle_executors.pop(handler)
        except KeyError:
            return self.execution_features.get_executor(handler, self.block_store)

    def put_executor(self, handler, executor):
        if handler in self.idle_executors:
            executor.cleanup()
        else:
            self.idle_executors[handler] = executor

class TaskSetExecutionRecord:

    def __init__(self, executor_cache, root_task_descriptor, block_store, master_proxy):
        self._lock = Lock()
        self.task_records = []
        self.current_task = None
        self.current_td = None
        self.block_store = block_store
        self.master_proxy = master_proxy
        self.executor_cache = executor_cache
        self.reference_cache = dict([(ref.id, ref) for ref in root_task_descriptor["inputs"]])
        self.initial_td = root_task_descriptor
        self.task_graph = LocalTaskGraph(self.initial_td["task_id"])
        self.job_output = LocalJobOutput(self.initial_td["expected_outputs"])
        for ref in self.initial_td["expected_outputs"]:
            self.task_graph.subscribe(ref, self.job_output)
        self.task_graph.spawn_and_publish([self.initial_td], self.initial_td["inputs"])

    def run(self):
        ciel.log.error("Running taskset starting at %s" % self.initial_td["task_id"], "TASKEXEC", logging.INFO)
        while not self.job_output.is_complete():
            try:
                next_td = self.task_graph.get_runnable_task()
            except IndexError:
                ciel.log.error("No more runnable tasks", "TASKEXEC", logging.INFO)
                break
            next_td["inputs"] = [self.retrieve_ref(ref) for ref in next_td["dependencies"]]
            task_record = TaskExecutionRecord(next_td, self, self.executor_cache, self.block_store, self.master_proxy)
            with self._lock:
                self.current_task = task_record
                self.current_td = next_td
            task_record.run()
            with self._lock:
                self.current_task.cleanup()
                self.current_task = None
                self.current_td = None
            self.task_graph.spawn_and_publish(task_record.spawned_tasks, task_record.published_refs, next_td)
            self.task_records.append(task_record)
        ciel.log.error("Taskset complete", "TASKEXEC", logging.INFO)

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

    def abort_task(self, task_id):
        with self._lock:
            if self.current_td["task_id"] == task_id:
                self.current_task.executor.abort()

    def notify_streams_done(self, task_id):
        with self._lock:
            if self.current_td["task_id"] == task_id:
                self.current_task.executor.notify_streams_done()

class TaskExecutionRecord:

    def __init__(self, task_descriptor, task_set, executor_cache, block_store, master_proxy):
        self.published_refs = []
        self.spawned_tasks = []
        self.spawn_counter = 0
        self.task_descriptor = task_descriptor
        self.task_set = task_set
        self.executor_cache = executor_cache
        self.block_store = block_store
        self.master_proxy = master_proxy
        if "package_ref" in task_descriptor["task_private"]:
            self.package_ref = task_descriptor["task_private"]["package_ref"]
        else:
            self.package_ref = None
        self.executor = self.executor_cache.get_executor(task_descriptor["handler"])

    def run(self):
        ciel.engine.publish("worker_event", "Start execution " + repr(self.task_descriptor['task_id']) + " with handler " + self.task_descriptor['handler'])
        ciel.log.error("Starting task %s with handler %s" % (str(self.task_descriptor['task_id']), self.task_descriptor['handler']), 'TASK', logging.INFO, False)
        try:
            self.executor.run(self.task_descriptor, self)
            ciel.engine.publish("worker_event", "Completed execution " + repr(self.task_descriptor['task_id']))
            ciel.log.error("Completed task %s with handler %s" % (str(self.task_descriptor['task_id']), self.task_descriptor['handler']), 'TASK', logging.INFO, False)
        except:
            ciel.log.error("Error in task %s with handler %s" % (str(self.task_descriptor['task_id']), self.task_descriptor['handler']), 'TASK', logging.ERROR, True)

    def cleanup(self):
        self.executor_cache.put_executor(self.task_descriptor["handler"], self.executor)

    def publish_ref(self, ref):
        self.published_refs.append(ref)
        self.task_set.publish_ref(ref)

    def prepublish_refs(self, refs):
        # I don't put these in the ref-cache now because local-master operation is currently single-threaded.
        self.master_proxy.publish_refs(self.task_descriptor["task_id"], refs)

    def create_spawned_task_name(self):
        sha = hashlib.sha1()
        sha.update('%s:%d' % (self.task_descriptor["task_id"], self.spawn_counter))
        ret = sha.hexdigest()
        self.spawn_counter += 1
        return ret

    def spawn_task(self, new_task_descriptor, **args):
        new_task_descriptor["task_id"] = self.create_spawned_task_name()
        if "dependencies" not in new_task_descriptor:
            new_task_descriptor["dependencies"] = []
        if "task_private" not in new_task_descriptor:
            new_task_descriptor["task_private"] = dict()
                     
        executor_class = self.executor_cache.execution_features.get_executor_class(new_task_descriptor["handler"])
        # Throws a BlameUserException if we can quickly determine the task descriptor is bad
        executor_class.build_task_descriptor(new_task_descriptor, self, self.block_store, **args)
        self.spawned_tasks.append(new_task_descriptor)
        return new_task_descriptor

    def retrieve_ref(self, ref):
        return self.task_set.retrieve_ref(ref)

