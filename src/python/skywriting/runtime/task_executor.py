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
from threading import Lock
import logging
import hashlib
import ciel

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, skypybase, stdlibbase, block_store, master_proxy, execution_features, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
        self.block_store = block_store
        self.master_proxy = master_proxy
        self.execution_features = execution_features
        self.skypybase = skypybase
        self.stdlibbase = stdlibbase

        self.root_executor = None
        self.root_handler = None
        self._lock = Lock()

        self.reset()
    
    # Out-of-thread asynchronous notification calls

    def abort_task(self, task_id):
        with self._lock:
            if self.root_task_id == task_id:
                self.root_executor.abort()
            self.root_task_id = None
            self.current_task_execution_record = None

    def notify_streams_done(self, task_id):
        with self._lock:
            # Guards against changes to self.current_{task_id, task_execution_record}
            if self.root_task_id == task_id:
                # Note on threading: much like aborts, the execution_record's execute() is running
                # in another thread. It might have not yet begun, already completed, or be in progress.
                self.root_executor.notify_streams_done()
    
    # Helper functions for main

    def run_task_with_executor(self, task_descriptor, executor):
        ciel.engine.publish("worker_event", "Start execution " + repr(task_descriptor['task_id']) + " with handler " + task_descriptor['handler'])
        ciel.log.error("Starting task %s with handler %s" % (str(task_descriptor['task_id']), task_descriptor['handler']), 'TASK', logging.INFO, False)
        try:
            executor.run(task_descriptor)
            ciel.engine.publish("worker_event", "Completed execution " + repr(task_descriptor['task_id']))
            ciel.log.error("Completed task %s with handler %s" % (str(task_descriptor['task_id']), task_descriptor['handler']), 'TASK', logging.INFO, False)
        except:
            ciel.log.error("Error in task %s with handler %s" % (str(task_descriptor['task_id']), task_descriptor['handler']), 'TASK', logging.ERROR, True)

    def spawn_all(self):
        if len(self.spawned_tasks) == 0:
            return
        self.master_proxy.spawn_tasks(self.root_task_id, self.spawned_tasks)

    def create_spawned_task_name(self):
        sha = hashlib.sha1()
        sha.update('%s:%d' % (self.root_task_id, self.spawn_counter))
        ret = sha.hexdigest()
        self.spawn_counter += 1
        return ret

    def commit(self):
        commit_bindings = dict([(ref.id, ref) for ref in self.published_refs])
        self.master_proxy.commit_task(self.root_task_id, commit_bindings)

    def reset(self):
        self.published_refs = []
        self.spawned_tasks = []
        self.reference_cache = None
        self.task_for_output_id = dict()
        self.spawn_counter = 0
        with self._lock:
            self.root_task_id = None

    # Main entry point

    def handle_input(self, input):

        new_task_handler = input["handler"]
        with self._lock:
            if self.root_handler != new_task_handler:
                if self.root_executor is not None:
                    self.root_executor.cleanup()
                self.root_executor = None
            if self.root_executor is None:
                self.root_executor = self.execution_features.get_executor(new_task_handler, self)
            self.root_handler = new_task_handler
            self.root_task_id = input["task_id"]

        self.reference_cache = dict([(ref.id, ref) for ref in input["inputs"]])
        if "package_ref" in input["task_private"]:
            self.package_ref = input["task_private"]["package_ref"]
        else:
            self.package_ref = None
        self.run_task_with_executor(input, self.root_executor)
        self.commit()
        self.spawn_all()
        self.reset()

    # Callbacks for executors

    def publish_ref(self, ref):
        self.published_refs.append(ref)
        self.reference_cache[ref.id] = ref

    def spawn_task(self, new_task_descriptor, **args):
        new_task_descriptor["task_id"] = self.create_spawned_task_name()
        if "dependencies" not in new_task_descriptor:
            new_task_descriptor["dependencies"] = []
        if "task_private" not in new_task_descriptor:
            new_task_descriptor["task_private"] = dict()
        target_executor = self.execution_features.get_executor(new_task_descriptor["handler"], self)
        # Throws a BlameUserException if we can quickly determine the task descriptor is bad
        target_executor.build_task_descriptor(new_task_descriptor, **args)
        # TODO here: use the master's task-graph apparatus.
        if "hint_small_task" in new_task_descriptor:
            for output in new_task_descriptor['expected_outputs']:
                self.task_for_output_id[output] = new_task_descriptor
        self.spawned_tasks.append(new_task_descriptor)
        return new_task_descriptor

    def resolve_ref(self, ref):
        if ref.is_consumable():
            return ref
        else:
            try:
                return self.reference_cache[ref.id]
            except KeyError:
                raise ReferenceUnavailableException(ref)

    def retrieve_ref(self, ref):
        # For the time being ignore the hint_small_task directive.
#        try:
        return self.resolve_ref(ref)
#        except ReferenceUnavailableException as e:
            # Try running a small task to generate the required reference
#            try:
#                producer_task = task_for_output_id[id]
                # Presence implies hint_small_task: we should run this now
#            except KeyError:
#                raise e
            # Try to resolve all the child's dependencies
  #          try:
#                producer_task["inputs"] = dict()
#                for child_ref in producer_task["dependencies"]:
#                    producer_task["inputs"][child_ref.id] = self.resolve_ref(child_ref)
#            except ReferenceUnavailableException:
                # Child can't run now
#                del producer_task["inputs"]
#                raise e
#            nested_executor = self.execution_features.get_executor(producer_task["handler"], self)
#            self.run_task_with_executor(producer_task, nested_executor)
            # Okay the child has run, and may or may not have defined its outputs.
            # If it hasn't, this will throw appropriately
#            return self.resolve_ref(ref)

