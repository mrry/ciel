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
from skywriting.lang.parser import CloudScriptParser
import urlparse
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from skywriting.lang.context import SimpleContext, TaskContext,\
    LambdaFunction
from skywriting.lang.datatypes import all_leaf_values, map_leaf_values
from skywriting.lang.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from skywriting.lang import ast
from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    FeatureUnavailableException, ExecutionInterruption,\
    SelectException, MissingInputException, MasterNotRespondingException,\
    RuntimeSkywritingError, BlameUserException
from threading import Lock
import cherrypy
import logging
import uuid
import hashlib
import subprocess
import pickle
import os.path
from skywriting.runtime.references import SWDataValue, SWURLReference,\
    SWRealReference,\
    SWErrorReference, SW2_FutureReference,\
    SW2_ConcreteReference

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, skypybase, block_store, master_proxy, execution_features, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
        self.block_store = block_store
        self.master_proxy = master_proxy
        self.execution_features = execution_features
        self.skypybase = skypybase

        self.current_task_id = None
        self.current_task_execution_record = None
    
        self._lock = Lock()
    
    def abort_task(self, task_id):
        with self._lock:
            if self.current_task_id == task_id:
                self.current_task_execution_record.abort()
            self.current_task_id = None
            self.current_task_execution_record = None

    def notify_streams_done(self, task_id):
        with self._lock:
            # Guards against changes to self.current_{task_id, task_execution_record}
            if self.current_task_id == task_id:
                # Note on threading: much like aborts, the execution_record's execute() is running
                # in another thread. It might have not yet begun, already completed, or be in progress.
                self.current_task_execution_record.notify_streams_done()
    
    def handle_input(self, input):
        handler = input['handler']

        if handler == 'swi':
            execution_record = SWInterpreterTaskExecutionRecord(input, self)
        elif handler == "skypy":
            execution_record = SWSkyPyTaskExecutionRecord(input, self)
        else:
            execution_record = SWExecutorTaskExecutionRecord(input, self)

        with self._lock:
            self.current_task_id = input['task_id']
            self.current_task_execution_record = execution_record
        
        cherrypy.engine.publish("worker_event", "Start execution " + repr(input['task_id']) + " with handler " + input['handler'])
        cherrypy.log.error("Starting task %s with handler %s" % (str(self.current_task_id), handler), 'TASK', logging.INFO, False)
        try:
            execution_record.execute()
            cherrypy.engine.publish("worker_event", "Completed execution " + repr(input['task_id']))
            cherrypy.log.error("Completed task %s with handler %s" % (str(self.current_task_id), handler), 'TASK', logging.INFO, False)
        except:
            cherrypy.log.error("Error in task %s with handler %s" % (str(self.current_task_id), handler), 'TASK', logging.ERROR, True)

        with self._lock:
            self.current_task_id = None
            self.current_task_execution_record = None
            
            
class ReferenceTableEntry:
    
    def __init__(self, reference):
        self.reference = reference
        self.is_dereferenced = False
        self.is_execd = False
        self.is_returned = False
        
    def __repr__(self):
        return 'ReferenceTableEntry(%s, d=%s, e=%s, r=%s)' % (repr(self.reference), repr(self.is_dereferenced), repr(self.is_execd), repr(self.is_returned))
        
class SpawnListEntry:
    
    def __init__(self, id, task_descriptor, continuation=None):
        self.id = id
        self.task_descriptor = task_descriptor
        self.continuation = continuation
        self.ignore = False
    
class SWContinuation:
    
    def __init__(self, task_stmt, context):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = context
        self.reference_table = {}
      
    def __repr__(self):
        return "SWContinuation(task_stmt=%s, current_local_id_index=%s, stack=%s, context=%s, reference_table=%s)" % (repr(self.task_stmt), repr(self.current_local_id_index), repr(self.stack), repr(self.context), repr(self.reference_table))

    def create_tasklocal_reference(self, ref):
        id = self.current_local_id_index
        self.current_local_id_index += 1
        self.reference_table[id] = ReferenceTableEntry(ref)
        return SWLocalReference(id)

    def store_tasklocal_reference(self, id, ref):
        """Used when copying references to a spawned continuation."""
        self.reference_table[id] = ReferenceTableEntry(ref)
        self.current_local_id_index = max(self.current_local_id_index, id + 1)
    
    # The following methods capture why we might have blocked on something,
    # for appropriate handling on task loading.
    def mark_as_dereferenced(self, ref):
        self.reference_table[ref.index].is_dereferenced = True
    def is_marked_as_dereferenced(self, id):
        return self.reference_table[id].is_dereferenced
    def mark_as_execd(self, ref):
        self.reference_table[ref.index].is_execd = True
    def is_marked_as_execd(self, id):
        return self.reference_table[id].is_execd
    def mark_as_returned(self, ref):
        self.reference_table[ref.index].is_returned = True
    def is_marked_as_returned(self, id):
        return self.reference_table[id].is_returned
        
    def rewrite_reference(self, id, real_ref):
        self.reference_table[id].reference = real_ref
        
    def resolve_tasklocal_reference_with_index(self, index):
        return self.reference_table[index].reference
    def resolve_tasklocal_reference_with_ref(self, ref):
        return self.reference_table[ref.index].reference

class SafeLambdaFunction(LambdaFunction):
    
    def __init__(self, function, interpreter):
        LambdaFunction.__init__(self, function)
        self.interpreter = interpreter

    def call(self, args_list, stack, stack_base, context):
        safe_args = self.interpreter.do_eager_thunks(args_list)
        return LambdaFunction.call(self, safe_args, stack, stack_base, context)

class SWLocalReference:
    """
    A primitive object used in the interpreter, and returned from functions like
    ref() and spawn(). Contains an index into the continuation's reference table,
    which identifies the real reference object.
    """
    
    def __init__(self, index):
        self.index = index
        
    def as_tuple(self):
        return ('local', self.index)

    def __repr__(self):
        return 'SWLocalReference(%d)' % self.index

class SWExecutorTaskExecutionRecord:
    
    def __init__(self, task_descriptor, task_executor):
        self.task_id = task_descriptor['task_id']
        try:
            self.original_task_id = task_descriptor['original_task_id']
        except KeyError:
            self.original_task_id = self.task_id
        self.expected_outputs = task_descriptor['expected_outputs'] 
        self.task_executor = task_executor
        self.inputs = task_descriptor['inputs']
        self.executor_name = task_descriptor['handler']
        self.executor = None
        self.is_running = True
        self.published_refs = []
        self._lock = Lock()
    
    def abort(self):
        self.is_running = False
        with self._lock:
            # Guards against missing abort because the executor is started between test and call
            if self.executor is not None:
                self.executor.abort()

    def notify_streams_done(self):
        # Out-of-thread call
        with self._lock:
            if self.executor is not None:
                self.executor.notify_streams_done()
    
    def fetch_executor_args(self, inputs):
        
        self.exec_args = self.task_executor.block_store.retrieve_object_for_ref(inputs["_args"], 'pickle')
        self.inputs = inputs
        
    # Callback for executor to register knowledge about a reference
    def publish_ref(self, ref):
        self.published_refs.append(ref)
        
    def commit(self):
        commit_bindings = dict([(ref.id, ref) for ref in self.published_refs])
        self.task_executor.master_proxy.commit_task(self.task_id, commit_bindings)
    
    def execute(self):        
        try:
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Fetching args")
                parsed_args = self.fetch_executor_args(self.inputs)
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Fetching executor")
                executor = self.task_executor.execution_features.get_executor(self.executor_name)
                with self._lock:
                    self.executor = executor
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Rewriting references")
                # A misleading name: this is just an environment that reads out of a dict.
                fake_cont = SkyPyRefCacheContinuation(self.inputs)
                # Rewrite refs this executor needs using our inputs as a name table
                self.executor.resolve_args_refs(self.exec_args, fake_cont)
                # No need to check args: they were checked earlier by whoever made this task
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Executing")
                self.executor.execute(self.task_executor.block_store, 
                                      self.task_id, 
                                      self.exec_args, 
                                      self.expected_outputs, 
                                      self.publish_ref,
                                      self.task_executor.master_proxy)

            if self.is_running:
                cherrypy.engine.publish("worker_event", "Committing")
                self.commit()
            else:
                self.task_executor.master_proxy.failed_task(self.task_id)
        except MissingInputException as mie:
            cherrypy.log.error('Missing input during SWI task execution', 'SWI', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'MISSING_INPUT', bindings=mie.bindings)
        except:
            cherrypy.log.error('Error during executor task execution', 'EXEC', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'RUNTIME_EXCEPTION')

class SWSkyPyTaskExecutionRecord:
    
    def __init__(self, task_descriptor, task_executor):
        self.task_id = task_descriptor['task_id']
        self.task_executor = task_executor
        
        self.is_running = True
        self.is_fetching = False
        self.is_interpreting = False
        
        try:
            self.interpreter = SWSkyPyTask(task_descriptor, self.task_executor.block_store, self.task_executor.execution_features, self.task_executor.master_proxy, self.task_executor.skypybase)
        except:
            cherrypy.log.error('Error during SkyPy task creation', 'SKYPY', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id)

    def execute(self):
        try:
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Fetching args")
                self.interpreter.fetch_inputs(self.task_executor.block_store)
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Interpreting")
                self.interpreter.interpret()
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Spawning")
                self.interpreter.spawn_all(self.task_executor.block_store, self.task_executor.master_proxy)
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Committing")
                self.interpreter.commit_result(self.task_executor.block_store, self.task_executor.master_proxy)
            else:
                self.task_executor.master_proxy.failed_task(self.task_id)
        
        except MissingInputException as mie:
            cherrypy.log.error('Missing input during SKYPY task execution', 'SKYPY', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'MISSING_INPUT', bindings=mie.bindings)
              
        except MasterNotRespondingException:
            cherrypy.log.error('Could not commit task results to the master', 'SKYPY', logging.ERROR, True)
                
        except:
            cherrypy.log.error('Error during SKYPY task execution', 'SKYPY', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'RUNTIME_EXCEPTION')    

    def abort(self):
        raise Exception("Not implemented")

class SkyPyBlankContinuation:
    
    def __init__(self):
        self.exec_deps = []
    def resolve_tasklocal_reference_with_ref(self, ref):
        return SW2_FutureReference(ref.id)
    def mark_as_execd(self, ref):
        # The executor will need this reference.
        self.exec_deps.append(ref.id)

class SkyPyRefCacheContinuation(SkyPyBlankContinuation):

    def __init__(self, refs_dict):
        SkyPyBlankContinuation.__init__(self)
        self.refs = refs_dict
    def resolve_tasklocal_reference_with_ref(self, ref):
        try:
            return self.refs[ref.id]
        except KeyError:
            return SkyPyBlankContinuation.resolve_tasklocal_reference_with_ref(self, ref)

class InterpreterTask:

    def __init__(self, task_descriptor, block_store, execution_features, master_proxy):

        ### Input parameters

        self.task_id = task_descriptor['task_id']
        self.master_proxy = master_proxy

        try:
            self.replay_uuid_list = task_descriptor['replay_uuids']
            self.replay_uuids = True
            self.current_uuid_index = 0
        except KeyError:
            self.replay_uuid_list = []
            self.replay_uuids = False

        self.block_store = block_store
        self.execution_features = execution_features
        
        try:
            self.original_task_id = task_descriptor['original_task_id']
            self.replay_ref = task_descriptor['replay_ref']
        except KeyError:
            self.original_task_id = self.task_id
            self.replay_ref = None

        self.expected_outputs = task_descriptor['expected_outputs']
        self.inputs = task_descriptor['inputs']

        ### Exit status

        self.spawn_list = []
        self.refs_to_publish = []
        self.halt_dependencies = []
        self.halt_feature_requirement = None
        self.save_cont_uri = None
        
        ### Transient local state

        self.is_running = True
        self.current_executor = None
        
        # A volatile cache of references fetched earlier this run, or supplied by the master at startup.
        self.reference_cache = dict()

    def fetch_inputs(self, block_store):

        self.reference_cache = self.inputs

    def spawn_all(self, block_store, master_proxy):

        if len(self.spawn_list) == 0:
            return
        
        spawn_descriptors = [x.task_descriptor for x in self.spawn_list]
        master_proxy.spawn_tasks(self.task_id, spawn_descriptors)

    def get_spawn_continuation_object_id(self, task_id):
        return '%s:cont' % (task_id, )

    def commit_result(self, block_store, master_proxy):
        
        commit_bindings = dict((ref.id, ref) for ref in self.refs_to_publish)
        master_proxy.commit_task(self.task_id, commit_bindings, self.save_cont_uri, self.replay_uuid_list)

    def spawn_exec_func(self, executor_name, args, new_task_id, exec_prefix, output_ids):

        # An environment that just returns FutureReferences whenever it's passed a SkyPyOpaqueReference
        fake_cont = SkyPyBlankContinuation()

        executor = self.execution_features.get_executor(executor_name)

        # Replace all relevant OpaqueReferences with FutureReferences, and log all ref IDs touched in the Continuation
        executor.resolve_args_refs(args, fake_cont)

        # Throw early if the args are bad
        executor.check_args_valid(args, output_ids)

        args_id = self.get_args_name_for_exec(exec_prefix)
        # Always assume the args-dict for an executor is fairly small.
        args_ref = SWDataValue(args_id, pickle.dumps(args))
        self.block_store.cache_object(args, "pickle", args_id)
        self.refs_to_publish.append(args_ref)
        
        inputs = dict([(refid, SW2_FutureReference(refid)) for refid in fake_cont.exec_deps])
        inputs['_args'] = SW2_FutureReference(args_ref.id)
        # I really know more about this, but that information is conveyed by a publish.
        # I try to always nominate Futures as dependencies so that I can easily convert to a set-of-deps rather than a dict.

        task_descriptor = {'task_id': new_task_id,
                           'handler': executor_name, 
                           'dependencies': inputs,
                           'expected_outputs': output_ids}
        
        self.spawn_list.append(SpawnListEntry(new_task_id, task_descriptor))
        
        if len(self.spawn_list) > 20:
            self.spawn_all(self.block_store, self.master_proxy)
            self.spawn_list = []
    
    def get_args_name_for_exec(self, prefix):
        return '%sargs' % (prefix, )

    def create_output_names_for_exec(self, executor_name, real_args, num_outputs):
        sha = hashlib.sha1()
        self.hash_update_with_structure(sha, [real_args, num_outputs])
        prefix = '%s:%s:' % (executor_name, sha.hexdigest())
        return ['%s%d' % (prefix, i) for i in range(num_outputs)]


    # Callback for executors to announce that they know something about a reference.
    def publish_reference(self, ref):
        self.reference_cache[ref.id] = ref
        self.refs_to_publish.append(ref)

    def exec_func(self, executor_name, args, n_outputs):
        
        output_ids = self.create_output_names_for_exec(executor_name, args, n_outputs)

        fake_cont = SkyPyRefCacheContinuation(self.reference_cache)
        self.current_executor = self.execution_features.get_executor(executor_name)
        # Resolve all of our OpaqueReferences to RealReferences, 
        # and as a side-effect record all refs touched in the Continuation.
        # Side-effect: modified args.
        self.current_executor.resolve_args_refs(args, fake_cont)
        # Might throw a BlameUserException.
        self.current_executor.check_args_valid(args, output_ids)
        # Eagerly check all references for availability.
        for refid in fake_cont.exec_deps:
            if refid not in self.reference_cache:
                self.halt_dependencies.extend(fake_cont.exec_deps)
                raise ReferenceUnavailableException(SW2_FutureReference(refid), None)

        # Might raise runtime errors
        self.current_executor.execute(self.block_store, 
                                      self.task_id, 
                                      args, 
                                      output_ids,
                                      self.publish_reference,
                                      self.master_proxy)

        self.current_executor = None
        return output_ids

    def deref_json(self, id):
        cherrypy.log.error("Deref-as-JSON %s" % id, "SKYPY", logging.INFO)
        if id not in self.reference_cache:
            raise ReferenceUnavailableException(SW2_FutureReference(id), None)
        else:
            real_ref = self.reference_cache[id]
            obj = self.block_store.retrieve_object_for_ref(real_ref, "json")
            def opaqueify_ref(x):
                if isinstance(x, SWRealReference):
                    self.reference_cache[x.id] = x
                    return self.SkyPyOpaqueReference(x.id)
                else:
                    return x
            return map_leaf_values(opaqueify_ref, obj)

    def hash_update_with_structure(self, hash, value):
        """
        Recurses over a Skywriting data structure (containing lists, dicts and 
        primitive leaves) in a deterministic order, and updates the given hash with
        all values contained therein.
        """
        if isinstance(value, list):
            hash.update('[')
            for element in value:
                self.hash_update_with_structure(hash, element)
                hash.update(',')
            hash.update(']')
        elif isinstance(value, dict):
            hash.update('{')
            for (dict_key, dict_value) in sorted(value.items()):
                hash.update(dict_key)
                hash.update(':')
                self.hash_update_with_structure(hash, dict_value)
                hash.update(',')
            hash.update('}')
        elif isinstance(value, SW2_ConcreteReference) or isinstance(value, SW2_FutureReference):
            hash.update('ref')
            hash.update(value.id)
        elif isinstance(value, SWURLReference):
            hash.update('ref')
            hash.update(value.urls[0])
        elif isinstance(value, SWDataValue):
            hash.update('ref*')
            hash.update(hash, value.value)
        else:
            hash.update(str(value))

class SWSkyPyTask:
    
    def __init__(self, task_descriptor, block_store, execution_features, master_proxy, skypybase):

        InterpreterTask.__init__(self, task_descriptor, block_store, execution_features, master_proxy)

        self.skypybase = skypybase
        
        ### Import SkyPy's internal reference descriptor, to save a rewriting stage

        import sys
        # Skypy's reference descriptor:
        sys.path.append(self.skypybase)
        from opaque_ref import SkyPyOpaqueReference
        self.SkyPyOpaqueReference = SkyPyOpaqueReference
        sys.path = sys.path[:-1]
        
    def fetch_inputs(self, block_store):

        InterpreterTask.fetch_inputs(self, block_store)
        
        coroutine_ref = None
        self.pyfile_ref = None

        try:
            coroutine_ref = self.reference_cache["_coro"]
            self.pyfile_ref = self.reference_cache["_py"]
        except KeyError:
            raise Exception("SkyPy tasks must have a _coro and a _py input")

        rq_list = [coroutine_ref, self.pyfile_ref]

        filenames = block_store.retrieve_filenames_for_refs_eager(rq_list)

        self.coroutine_filename = filenames[0]
        self.py_source_filename = filenames[1]
        
        cherrypy.log.error('Fetched coroutine image to %s, .py source to %s' 
                           % (self.coroutine_filename, self.py_source_filename), 'SKYPY', logging.INFO)

    def ref_from_pypy_dict(self, args_dict, refid):
        try:
            ref = SWDataValue(refid, args_dict["outstr"])
            cherrypy.log.error("SkyPy shipped a small ref (length %d)" 
                               % len(args_dict["outstr"]), "SKYPY", logging.INFO)
            return ref
        except KeyError:
            _, size = self.block_store.store_file(args_dict["filename"], refid, can_move=True)
            real_ref = SW2_ConcreteReference(refid, size)
            real_ref.add_location_hint(self.block_store.netloc)
            cherrypy.log.error("SkyPy shipped a large ref as file %s (length %d)" 
                               % (args_dict["filename"], size), "SKYPY", logging.INFO)
            return real_ref

    def str_from_pypy_dict(self, args_dict):
        try:
            return pickle.loads(args_dict["outstr"])
        except KeyError:
            with open(args_dict["filename"], "r") as f:
                return pickle.load(f)

    def interpret(self):

        pypy_args = ["pypy", 
                     self.skypybase + "/stub.py", 
                     "--resume_state", self.coroutine_filename, 
                     "--source", self.py_source_filename, 
                     "--taskid", self.task_id]

        pypy_process = subprocess.Popen(pypy_args, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        cont_task_id = None

        while True:
            
            request_args = pickle.load(pypy_process.stdout)
            request = request_args["request"]
            del request_args["request"]
            cherrypy.log.error("Request: %s" % request, "SKYPY", logging.DEBUG)
            if request == "deref":
                ref_ret = self.ref_as_file_or_string(**request_args)
                ret = None
                if ref_ret is None:
                    self.halt_dependencies.append(request_args["id"])
                    ret = {"success": False}
                else:
                    (is_inline_data, data) = ref_ret
                    if is_inline_data:
                        cherrypy.log.error("Pypy dereferenced %s, returning data inline" % request_args["id"],
                                           "SKYPY", logging.INFO)
                        ret = {"success": True, "strdata": data}
                    else:
                        cherrypy.log.error("Pypy dereferenced %s, returning %s" 
                                           % (request_args["id"], filename), "SKYPY", logging.INFO)
                        ret = {"success": True, "filename": data}
                pickle.dump(ret, pypy_process.stdin)
            elif request == "deref_json":
                try:
                    pickle.dump({"obj": self.deref_json(**request_args), "success": True}, pypy_process.stdin)
                except ReferenceUnavailableException:
                    self.halt_dependencies.append(request_args["id"])
                    pickle.dump({"success": False}, pypy_process.stdin)
            elif request == "spawn":
                self.spawn_func(**request_args)
            elif request == "spawn_exec":
                self.spawn_exec_func(**request_args)
            elif request == "exec":
                try:
                    output_ref_names = self.exec_func(**request_args)
                    pickle.dump({"success": True, "output_names": output_ref_names}, pypy_process.stdin)
                except ReferenceUnavailableException:
                    pickle.dump({"success": False}, pypy_process.stdin)
                except FeatureUnavailableException as exn:
                    pickle.dump({"success": False}, pypy_process.stdin)
                    self.halt_feature_requirement = exn.feature_name
                    # The interpreter will now snapshot its own state and freeze; on resumption it will retry the exec.
            elif request == "freeze":
                # The interpreter is stopping because it needed a reference that wasn't ready yet.
                cont_task_id = request_args["new_task_id"]
                cont_ref_id = self.get_spawn_continuation_object_id(cont_task_id)
                cont_ref = self.ref_from_pypy_dict(request_args, cont_ref_id)
                self.refs_to_publish.append(cont_ref)
                self.halt_dependencies.extend(list(request_args["additional_deps"]))
                cont_deps = dict([(k, SW2_FutureReference(k)) for k in self.halt_dependencies])
                cont_deps["_coro"] = SW2_FutureReference(cont_ref_id)
                cont_deps["_py"] = self.pyfile_ref
                cont_task_descriptor = {'task_id': str(cont_task_id),
                                        'handler': 'skypy',
                                        'dependencies': cont_deps,
                                        'expected_outputs': map(str, self.expected_outputs),
                                        'continues_task': str(self.original_task_id)}
                if self.halt_feature_requirement is not None:
                    cont_task_descriptor['require_features'] = [self.halt_feature_requirement]
                self.spawn_list.append(SpawnListEntry(cont_task_id, cont_task_descriptor))
                break
            elif request == "done":
                # The interpreter is stopping because the function has completed
                result_ref = self.ref_from_pypy_dict(request_args, self.expected_outputs[0])
                self.refs_to_publish.append(result_ref)
                break
            elif request == "exception":
                report_text = self.str_from_pypy_dict(request_args)
                raise Exception("Fatal pypy exception: %s" % report_text)

#       except MissingInputException as mie:
#            cherrypy.log.error('Cannot retrieve inputs for task: %s' % repr(mie.bindings), 'SKYPY', logging.WARNING)
#            raise

#        except Exception:
#            cherrypy.log.error('Interpreter error.', 'SKYPY', logging.ERROR, True)
#            self.save_continuation = True
#            raise

    def spawn_func(self, new_task_id, output_id, **otherargs):

        coro_ref = self.ref_from_pypy_dict(otherargs, self.get_spawn_continuation_object_id(new_task_id))
        
        new_task_deps = {"_py": self.pyfile_ref, "_coro": coro_ref}
        
        task_descriptor = {'task_id': new_task_id,
                           'handler': 'skypy',
                           'dependencies': new_task_deps,
                           'expected_outputs': [str(output_id)]
                          }

        self.spawn_list.append(SpawnListEntry(new_task_id, task_descriptor))

    def ref_as_file_or_string(self, id):
        cherrypy.log.error("Deref: %s" % id, "SKYPY", logging.INFO)
        try:
            ref = self.reference_cache[id]
            if isinstance(ref, SWDataValue):
                return (True, ref.value)
            else:
                filenames = self.block_store.retrieve_filenames_for_refs_eager([self.reference_cache[id]])
                return (False, filenames[0])
        except KeyError:
            return None

class SWInterpreterTaskExecutionRecord:
    
    def __init__(self, task_descriptor, task_executor):
        self.task_id = task_descriptor['task_id']
        self.task_executor = task_executor
        
        self.is_running = True
        self.is_fetching = False
        self.is_interpreting = False
        
        try:
            self.interpreter = SWRuntimeInterpreterTask(task_descriptor, self.task_executor.block_store, self.task_executor.execution_features, self.task_executor.master_proxy)
        except:
            cherrypy.log.error('Error during SWI task creation', 'SWI', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id)            

    def execute(self):
        try:
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Fetching args")
                self.interpreter.fetch_inputs(self.task_executor.block_store)
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Interpreting")
                self.interpreter.interpret()
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Spawning")
                self.interpreter.spawn_all(self.task_executor.block_store, self.task_executor.master_proxy)
            if self.is_running:
                cherrypy.engine.publish("worker_event", "Committing")
                self.interpreter.commit_result(self.task_executor.block_store, self.task_executor.master_proxy)
            else:
                self.task_executor.master_proxy.failed_task(self.task_id)
        
        except MissingInputException as mie:
            cherrypy.log.error('Missing input during SWI task execution', 'SWI', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'MISSING_INPUT', bindings=mie.bindings)
              
        except MasterNotRespondingException:
            cherrypy.log.error('Could not commit task results to the master', 'SWI', logging.ERROR, True)
                
        except:
            cherrypy.log.error('Error during SWI task execution', 'SWI', logging.ERROR, True)
            self.task_executor.master_proxy.failed_task(self.task_id, 'RUNTIME_EXCEPTION')    

    def abort(self):
        self.is_running = False
        self.interpreter.abort()
        
class SWRuntimeInterpreterTask:
    
    def __init__(self, task_descriptor, block_store, execution_features, master_proxy):

        InterpreterTask.__init__(self, task_descriptor, block_store, execution_features, master_proxy)

        try:
            self.save_continuation = task_descriptor['save_continuation']
        except KeyError:
            self.save_continuation = False

        self.spawn_counter = 0
        self.continuation = None
        self.result = None
        
    def abort(self):
        self.is_running = False
        if self.current_executor is not None:
            try:
                self.current_executor.abort()
            except:
                pass
            
    def fetch_inputs(self, block_store):

        InterpreterTask.fetch_inputs(self, block_store)

        self.continuation = self.block_store.retrieve_object_for_ref(self.inputs["_cont"], 'pickle')
        
        # TODO: Figure out what to do about SWI's old eager-fetch-on-startup policy,
        # in which it would eagerly retrieve anything that had been lazily deref'd before the last halt.

        #fetch_refs = [ref for (local_id, ref) in fetch_objects]
        #fetched_objects = self.block_store.retrieve_objects_for_refs(fetch_refs, 'json')

        #for (id, ref, ob) in zip([local_id for (local_id, ref) in fetch_objects], fetch_refs, fetched_objects):
        #   self.continuation.rewrite_reference(id, SWDataValue(ref.id, ob))
            
        cherrypy.log.error('Fetched all task inputs', 'SWI', logging.INFO)

    def interpret(self):
        self.continuation.context.restart()
        task_context = TaskContext(self.continuation.context, self)
        
        task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.spawn_func(x[0], x[1])))
        task_context.bind_tasklocal_identifier("spawn_exec", LambdaFunction(lambda x: self.spawn_exec_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
        task_context.bind_tasklocal_identifier("int", SafeLambdaFunction(lambda x: int(x[0]), self))
        task_context.bind_tasklocal_identifier("range", SafeLambdaFunction(lambda x: range(*x), self))
        task_context.bind_tasklocal_identifier("len", SafeLambdaFunction(lambda x: len(x[0]), self))
        task_context.bind_tasklocal_identifier("has_key", SafeLambdaFunction(lambda x: x[1] in x[0], self))
        task_context.bind_tasklocal_identifier("get_key", SafeLambdaFunction(lambda x: x[0][x[1]] if x[1] in x[0] else x[2], self))
        task_context.bind_tasklocal_identifier("exec", LambdaFunction(lambda x: self.exec_func(x[0], x[1], x[2])))
        #task_context.bind_tasklocal_identifier("ref", LambdaFunction(lambda x: self.make_reference(x)))
        #task_context.bind_tasklocal_identifier("is_future", LambdaFunction(lambda x: self.is_future(x[0])))
        #task_context.bind_tasklocal_identifier("is_error", LambdaFunction(lambda x: self.is_error(x[0])))
        #task_context.bind_tasklocal_identifier("abort", LambdaFunction(lambda x: self.abort_production(x[0])))
        #task_context.bind_tasklocal_identifier("task_details", LambdaFunction(lambda x: self.get_task_details(x[0])))
        #task_context.bind_tasklocal_identifier("select", LambdaFunction(lambda x: self.select_func(x[0]) if len(x) == 1 else self.select_func(x[0], x[1])))
        visitor = StatementExecutorVisitor(task_context)
        
        try:
            self.result = visitor.visit(self.continuation.task_stmt, self.continuation.stack, 0)

            # The script finished successfully

            json_result = simplejson.dumps(self.result, cls=SWReferenceJSONEncoder)
            block_store.cache_object(self.result, "json", self.expected_outputs[0])
            if len(json_result) < 128:
                result_ref = SWDataValue(self.expected_outputs[0], json_result)
            else:
                block_store.store_object(json_result, "noop", self.expected_outputs[0])
                result_ref = SW2_ConcreteReference(self.expected_outputs[0], size_hint)
                result_ref.add_location_hint(self.block_store.netloc)   
                
            self.publish_reference(result_ref)

            # XXX: This is for the unusual case that we have a task fragment that runs to completion without returning anything.
            #      Could maybe use an ErrorRef here, but this might not be erroneous if, e.g. the interactive shell is used.
            if self.result is None:
                self.result = SWErrorReference('NO_RETURN_VALUE', 'null')

            if self.save_continuation:
                self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 'pickle', self.get_saved_continuation_object_id())

#        except SelectException, se:
#            
#            local_select_group = se.select_group
#            timeout = se.timeout
#            
#            select_group = map(self.continuation.resolve_tasklocal_reference_with_ref, local_select_group)
#                        
#            cont_task_id = self.create_spawned_task_name()
#                        
#            cont_task_descriptor = {'task_id': str(cont_task_id),
#                                    'handler': 'swi',
#                                    'dependencies': {},
#                                    'select_group': select_group,
#                                    'select_timeout': timeout,
#                                    'expected_outputs': map(str, self.expected_outputs),
#                                    'save_continuation': self.save_continuation}
#            self.save_continuation = False
#            self.spawn_list.append(SpawnListEntry(cont_task_id, cont_task_descriptor, self.continuation))
            
        except ExecutionInterruption, ei:

            # Need to add a continuation task to the spawn list.
            cont_deps = dict([(ref.id, ref) for ref in self.halt_dependencies])
            cont_task_id = self.create_spawned_task_name()
            spawned_cont_id = self.get_spawn_continuation_object_id(cont_task_id)
            _, size_hint = block_store.store_object(self.continuation, 'pickle', spawned_cont_id)
            spawned_cont_ref = SW2_ConcreteReference(spawned_cont_id, size_hint)
            spawned_cont_ref.add_location_hint(self.block_store.netloc)
            self.publish_reference(spawned_cont_ref)
            cont_deps['_cont'] = SW2_FutureReference(spawned_cont_id)

            cont_task_descriptor = {'task_id': str(cont_task_id),
                                    'handler': 'swi',
                                    # TODO: How much can we share this with Skypy?
                                    'dependencies': cont_deps, # _cont will be added at spawn time.
                                    'expected_outputs': map(str, self.expected_outputs),
                                    'save_continuation': self.save_continuation,
                                    'continues_task': str(self.original_task_id)}
            self.save_continuation = False
            if isinstance(ei, FeatureUnavailableException):
                cont_task_descriptor['require_features'] = [ei.feature_name]
            
            self.spawn_list.append(SpawnListEntry(cont_task_id, cont_task_descriptor))
            return
            
        except MissingInputException as mie:
            cherrypy.log.error('Cannot retrieve inputs for task: %s' % repr(mie.bindings), 'SWI', logging.WARNING)
            raise

        except Exception:
            cherrypy.log.error('Interpreter error.', 'SWI', logging.ERROR, True)
            self.save_continuation = True
            raise

    def get_spawn_continuation_object_id(self, task_id):
        return '%s:cont' % (task_id, )

    def get_saved_continuation_object_id(self):
        return '%s:saved_cont' % (self.task_id, )

    def build_spawn_continuation(self, spawn_expr, args):
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        cont = SWContinuation(spawned_task_stmt, SimpleContext())
        
        # Now need to build the reference table for the spawned task.
        captured_refs = set()
        
        # Local references in the arguments.
        for leaf in filter(lambda x: isinstance(x, SWRealReference), all_leaf_values(args)):
            captured_refs.add(leaf.id)
            
        # Local references captured in the lambda/function.
        for leaf in filter(lambda x: isinstance(x, SWRealReference), all_leaf_values(spawn_expr.captured_bindings)):
            captured_refs.add(leaf.id)

        # Actually build the new reference table.
        # TODO: This would be better if we compressed the table, but might take a while.
        #       So let's assume that we won't run out of indices in a normal run :).
        for id in captured_refs:
            try:
                cont.reference_table[id] = self.continuation.reference_table[id]
            except KeyError:
                try:
                    cont.reference_table[id] = self.reference_cache[id]
                except KeyError:
                    pass
        
        return cont

    def create_spawned_task_name(self):
        sha = hashlib.sha1()
        sha.update('%s:%d' % (self.task_id, self.spawn_counter))
        ret = sha.hexdigest()
        self.spawn_counter += 1
        return ret
    
    def create_spawn_output_name(self, task_id):
        return 'swi:%s' % task_id
    
    def spawn_func(self, spawn_expr, args):

        args = self.do_eager_thunks(args)

        # Create new continuation for the spawned function.
        spawned_continuation = self.build_spawn_continuation(spawn_expr, args)
        
        # Append the new task definition to the spawn list.
        new_task_id = self.create_spawned_task_name()
        expected_output_id = self.create_spawn_output_name(new_task_id)
        
        # Match up the output with a new tasklocal reference.
        ret = SW2_FutureReference(expected_output_id)

        spawned_cont_id = self.get_spawn_continuation_object_id(new_task_id)
        _, size_hint = block_store.store_object(spawned_continuation, 'pickle', spawned_cont_id)
        spawned_cont_ref = SW2_ConcreteReference(spawned_cont_id, size_hint)
        spawned_cont_ref.add_location_hint(self.block_store.netloc)
        self.publish_reference(spawned_cont_ref)
        
        task_descriptor = {'task_id': new_task_id,
                           'handler': 'swi',
                           'dependencies': {"_cont": SW2_FutureReference(spawned_cont_id)},
                           'expected_outputs': [str(expected_output_id)]
                          }
        
        # TODO: we could visit the spawn expression and try to guess what requirements
        #       and executors we need in here. 
        # TODO: should probably look at dereference wrapper objects in the spawn context
        #       and ship them as inputs.
        
        self.spawn_list.append(SpawnListEntry(new_task_id, task_descriptor))

        # Return new future reference to the interpreter
        return ret

    def do_eager_thunks(self, args):

        def resolve_thunks_mapper(leaf):
            if isinstance(leaf, SWDereferenceWrapper):
                return self.eager_dereference(leaf.ref, id_to_result_map)
            else:
                return leaf

        return map_leaf_values(resolve_thunks_mapper, args)

    def spawn_exec_func(self, executor_name, exec_args, num_outputs):

        args = self.do_eager_thunks(exec_args)

        new_task_id = self.create_spawned_task_name()
        exec_prefix = get_exec_prefix(executor_name, args, num_outputs)
        _, expected_output_ids = self.create_names_for_exec(executor_name, args, num_outputs)

        return InterpreterTask.spawn_exec_func(self, executor_name, args, new_task_id, exec_prefix, output_ids)

    def get_exec_prefix(self, executor_name, real_args, num_outputs):
        sha = hashlib.sha1()
        self.hash_update_with_structure(sha, [real_args, num_outputs])
        return '%s:%s:' % (executor_name, sha.hexdigest())

    def create_names_for_exec(self, executor_name, real_args, num_outputs):
        prefix = self.get_exec_prefix(executor_name, real_args, num_outputs)
        args_name = '%sargs' % (prefix, )
        output_names = ['%s%d' % (prefix, i) for i in range(num_outputs)]
        return args_name, output_names
    
    def exec_func(self, executor_name, args, num_outputs):
        
        real_args = self.do_eager_thunks(args)
        return InterpreterTask.exec_func(self, executor_name, real_args, num_outputs)

    #def make_reference(self, urls):
    #   return self.continuation.create_tasklocal_reference(SWURLReference(urls))

    def lazy_dereference(self, ref):
        self.halt_dependencies.append(ref.id)
        return SWDereferenceWrapper(ref)

    def eager_dereference(self, ref):
        # For SWI, all decodes are JSON
        if isinstance(ref, SWDataValue):
            return simplejson.loads(real_ref.value, cls=SWReferenceJSONEncoder)
        else:
            self.halt_dependencies.append(ref.id)
            raise ReferenceUnavailableException(ref, self.continuation)

    def include_script(self, target_expr):
        if isinstance(target_expr, basestring):
            # Name may be relative to the local stdlib.
            target_expr = urlparse.urljoin('http://%s/stdlib/' % self.block_store.netloc, target_expr)
            target_ref = SWURLReference([target_expr])
        elif isinstance(target_expr, SWRealReference):    
            target_ref = target_expr
        else:
            raise BlameUserException('Invalid object %s passed as the argument of include', 'INCLUDE', logging.ERROR)

        try:
            script = self.block_store.retrieve_object_for_ref(target_ref, 'script')
        except:
            cherrypy.log.error('Error parsing included script', 'INCLUDE', logging.ERROR, True)
            raise BlameUserException('The included script did not parse successfully')
        return script

    def is_future(self, ref):
        return isinstance(ref, SW2_FutureReference)

    def is_error(self, ref):
        return isinstance(ref, SWErrorReference)

    def abort_production(self, ref):
        raise
        #real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        #if isinstance(real_ref, SWLocalFutureReference):
        #    self.spawn_list[real_ref.spawn_list_index].ignore = True
        #elif isinstance(real_ref, SWGlobalFutureReference):
        #    self.master_proxy.abort_production_of_output(real_ref)
        #return True
    
    def get_task_details(self, ref):
        raise
        #real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        #if isinstance(real_ref, SWGlobalFutureReference):
        #    return self.master_proxy.get_task_details_for_future(ref)
        #else:
        #    return {}

#    def select_func(self, select_group, timeout=None):
#        if self.select_result is not None:
#            return self.select_result
#        else:
#            raise SelectException(select_group, timeout)

    def hash_update_with_structure(self, hash, value):
        """
        Recurses over a Skywriting data structure (containing lists, dicts and 
        primitive leaves) in a deterministic order, and updates the given hash with
        all values contained therein.
        """
        if isinstance(value, list):
            hash.update('[')
            for element in value:
                self.hash_update_with_structure(hash, element)
                hash.update(',')
            hash.update(']')
        elif isinstance(value, dict):
            hash.update('{')
            for (dict_key, dict_value) in sorted(value.items()):
                hash.update(dict_key)
                hash.update(':')
                self.hash_update_with_structure(hash, dict_value)
                hash.update(',')
            hash.update('}')
        elif isinstance(value, SWLocalReference):
            self.hash_update_with_structure(hash, self.convert_tasklocal_to_real_reference(value))
        elif isinstance(value, SW2_ConcreteReference) or isinstance(value, SW2_FutureReference):
            hash.update('ref')
            hash.update(value.id)
        elif isinstance(value, SWURLReference):
            hash.update('ref')
            hash.update(value.urls[0])
        elif isinstance(value, SWDataValue):
            hash.update('ref*')
            hash.update(value.value)
        else:
            hash.update(str(value))

def map_leaf_values(f, value):
    """
    Recurses over a Skywriting data structure (containing lists, dicts and 
    primitive leaves), and returns a new structure with the leaves mapped as specified.
    """
    if isinstance(value, list):
        return map(lambda x: map_leaf_values(f, x), value)
    elif isinstance(value, dict):
        ret = {}
        for (dict_key, dict_value) in value.items():
            key = map_leaf_values(f, dict_key)
            value = map_leaf_values(f, dict_value)
            ret[key] = value
        return ret
    else:
        return f(value)

def accumulate_leaf_values(f, value):

    def flatten_lofl(ls):
        ret = []
        for l in ls:
            ret.extend(l)
        return ret

    if isinstance(value, list):
        accumd_list = [accumulate_leaf_values(f, v) for v in value]
        return flatten_lofl(accumd_list)
    elif isinstance(value, dict):
        accumd_keys = flatten_lofl([accumulate_leaf_values(f, v) for v in value.keys()])
        accumd_values = flatten_lofl([accumulate_leaf_values(f, v) for v in value.values()])
        accumd_keys.extend(accumd_values)
        return accumd_keys
    else:
        return [f(value)]
