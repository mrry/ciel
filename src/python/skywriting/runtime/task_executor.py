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
from skywriting.lang.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from skywriting.lang import ast
from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    FeatureUnavailableException, ExecutionInterruption,\
    SelectException, MissingInputException, MasterNotRespondingException,\
    RuntimeSkywritingError, BlameUserException
from skywriting.runtime.references import SWReferenceJSONEncoder
from threading import Lock
import cherrypy
import logging
import uuid
import hashlib
import subprocess
import pickle
import simplejson
import os.path
from shared.references import SWDataValue, SWRealReference,\
    SWErrorReference, SW2_FutureReference, SW2_ConcreteReference
from shared.exec_helpers import get_exec_prefix, get_exec_output_ids

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
            execution_record = SWInterpreterTaskExecutionRecord(input, self, SWRuntimeInterpreterTask)
        elif handler == "skypy":
            execution_record = SWInterpreterTaskExecutionRecord(input, self, SkyPyInterpreterTask, skypybase=self.skypybase)
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
            
class SpawnListEntry:
    
    def __init__(self, id, task_descriptor, continuation=None):
        self.id = id
        self.task_descriptor = task_descriptor
    
class RefCacheEnvironment:

    def __init__(self, refs_dict):
        self.refs = refs_dict
    def resolve_ref(self, ref):
        if ref.is_consumable():
            return ref
        else:
            try:
                return self.refs[ref.id]
            except KeyError:
                raise ReferenceUnavailableException(ref.id)

class RefList:
    
    def __init__(self):
        self.exec_deps = []
    def add_ref(self, ref):
        self.exec_deps.append(ref)

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
                env = RefCacheEnvironment(self.inputs)
                # Rewrite refs this executor needs using our inputs as a name table
                self.executor.resolve_required_refs(self.exec_args, env.resolve_ref)
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

class SWInterpreterTaskExecutionRecord:
    
    def __init__(self, task_descriptor, task_executor, interpreter_class, **interpreter_args):
        self.task_id = task_descriptor['task_id']
        self.task_executor = task_executor
        
        self.is_running = True
        self.is_fetching = False
        self.is_interpreting = False
        
        try:
            self.interpreter = interpreter_class(task_descriptor, self.task_executor.block_store, self.task_executor.execution_features, self.task_executor.master_proxy, **interpreter_args)
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
        self.spawn_counter = 0
        
        # A volatile cache of references fetched earlier this run, or supplied by the master at startup.
        self.reference_cache = dict()

    def fetch_inputs(self, block_store):

        self.reference_cache = self.inputs
        self.context = self.block_store.retrieve_object_for_ref(self.inputs["_generic_cont"], 'pickle')

    def interpret(self):

        try:

            successful_completion = self.run_interpreter()
            if successful_completion:
                self.refs_to_publish.append(self.result_ref)
                if self.save_continuation:
                    self.save_continuation()
            else:
                cont_deps = dict([(ref.id, ref) for ref in self.halt_dependencies])
                # Add interpreter-specific dependencies
                self.add_cont_deps(cont_deps)
                cont_task_descriptor = {'handler': self.handler_name,
                                        'dependencies': cont_deps,
                                        'expected_outputs': map(str, self.expected_outputs),
                                        'save_continuation': self.save_continuation,
                                        'continues_task': str(self.original_task_id)}
                if self.halt_feature_requirement is not None:
                    cont_task_descriptor['require_features'] = [self.halt_feature_requirement]
                    
                self.add_spawned_task(cont_task_descriptor)

        except MissingInputException as mie:
            cherrypy.log.error('Cannot retrieve inputs for task: %s' % repr(mie.bindings), 'SKYPY', logging.WARNING)
            raise
        except Exception:
            cherrypy.log.error('Interpreter error.', 'SKYPY', logging.ERROR, True)
            self.save_continuation = True
            raise

    def create_spawned_task_name(self):
        sha = hashlib.sha1()
        sha.update('%s:%d' % (self.task_id, self.spawn_counter))
        ret = sha.hexdigest()
        self.spawn_counter += 1
        return ret

    def add_spawned_task(self, descriptor):

        fresh_name = self.create_spawned_task_name()
        descriptor["task_id"] = fresh_name
        self.spawn_list.append(SpawnListEntry(fresh_name, descriptor))

        if len(self.spawn_list) > 20:
            self.spawn_all(self.block_store, self.master_proxy)
            self.spawn_list = []
    
    def spawn_task(self, new_task_deps, output_id):

        task_descriptor = {'handler': self.handler_name,
                           'dependencies': new_task_deps,
                           'expected_outputs': [str(output_id)]}

        self.add_spawned_task(task_descriptor)

    def spawn_all(self, block_store, master_proxy):

        if len(self.spawn_list) == 0:
            return
        
        spawn_descriptors = [x.task_descriptor for x in self.spawn_list]
        master_proxy.spawn_tasks(self.task_id, spawn_descriptors)

    def get_spawn_continuation_object_id(self):
        return '%s:%d:%spawncont' % (self.task_id, self.spawn_counter)

    def get_saved_continuation_object_id(self):
        return '%s:saved_cont' % (self.task_id, )

    def commit_result(self, block_store, master_proxy):
        
        commit_bindings = dict((ref.id, ref) for ref in self.refs_to_publish)
        master_proxy.commit_task(self.task_id, commit_bindings, self.save_cont_uri, self.replay_uuid_list)

    def spawn_exec_func(self, executor_name, args, n_outputs, exec_prefix):

        output_ids = get_exec_output_ids(exec_prefix)

        executor = self.execution_features.get_executor(executor_name)

        # Throw early if the args are bad
        executor.check_args_valid(args, output_ids)

        # Discover required ref IDs for this executor
        l = RefList()
        executor.get_required_refs(args, l.add_ref)

        args_id = self.get_args_name_for_exec(exec_prefix)
        output_ids = get_exec_output_ids(exec_prefix)

        args_ref = self.block_store.ref_from_object(args, "pickle", args_id)
        
        inputs = dict([(ref.id, ref) for ref in l.exec_deps])
        inputs['_args'] = args_ref

        task_descriptor = {'handler': executor_name, 
                           'dependencies': inputs,
                           'expected_outputs': output_ids}
        
        self.add_spawned_task(task_descriptor)
        
    def get_args_name_for_exec(self, prefix):
        return '%sargs' % (prefix, )

    def publish_reference(self, ref):
        self.refs_to_publish.append(ref)

    def exec_func(self, executor_name, args, n_outputs):
        
        exec_prefix = get_exec_prefix(executor_name, args, n_outputs)
        output_ids = get_exec_output_ids(exec_prefix, n_outputs)

        l = RefList()
        self.current_executor = self.execution_features.get_executor(executor_name)
        # Resolve all interpreter-specific references to RealReferences, 
        # and as a side-effect record all refs touched in the environment.
        # Might throw a BlameUserException.
        self.current_executor.check_args_valid(args, output_ids)
        self.current_executor.get_required_refs(args, l.add_ref)
        # Eagerly check all references for availability.
        raise_ref = None
        for ref in l.exec_deps:
            if not self.is_ref_resolvable(ref):
                self.halt_dependencies.append(ref)
                raise_ref = ref
        if raise_ref is not None:
            raise ReferenceUnavailableException(raise_ref)

        # Okay, all ref are good to go: executor should pull them in and run.
        env = RefCacheEnvironment(self.reference_cache)
        self.current_executor.resolve_required_refs(args, env.resolve_ref)

        # Might raise runtime errors
        self.current_executor.execute(self.block_store, 
                                      self.task_id, 
                                      args, 
                                      output_ids,
                                      lambda x: None,
                                      self.master_proxy)

        # lambda x: None is a dummy callback for publishing references-- there's no need to publish, as this
        # is a synchronous exec operation, so all references are born concrete.

        ret = self.current_executor.output_refs
        self.current_executor = None
        return ret
    
    def resolve_ref(self, ref):
        if not ref.is_consumable():
            try:
                return self.reference_cache[ref.id]
            except KeyError:
                raise ReferenceUnavailableException(ref)
        else:
            return ref

    def is_ref_resolvable(self, ref):
        try:
            self.resolve_ref(ref)
            return True
        except ReferenceUnavailableException:
            return False
    
    def deref_json(self, ref):
        cherrypy.log.error("Deref-as-JSON %s" % id, "INTERPRETER", logging.INFO)
        return self.block_store.retrieve_object_for_ref(self.resolve_ref(ref), "json")

class SkyPyInterpreterTask(InterpreterTask):
    
    def __init__(self, task_descriptor, block_store, execution_features, master_proxy, skypybase):

        InterpreterTask.__init__(self, task_descriptor, block_store, execution_features, master_proxy)
        self.skypybase = skypybase
        self.handler_name = "skypy"
        
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

    def run_interpreter(self):

        pypy_env = os.environ.copy()
        pypy_env["PYTHONPATH"] = self.skypybase + ":" + pypy_env["PYTHONPATH"]

        pypy_args = ["pypy", 
                     self.skypybase + "/stub.py", 
                     "--resume_state", self.coroutine_filename, 
                     "--source", self.py_source_filename, 
                     "--taskid", self.task_id]

        pypy_process = subprocess.Popen(pypy_args, env=pypy_env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        while True:
            
            request_args = pickle.load(pypy_process.stdout)
            request = request_args["request"]
            del request_args["request"]
            cherrypy.log.error("Request: %s" % request, "SKYPY", logging.DEBUG)
            if request == "deref":
                try:
                    ret = self.deref_func(**request_args)
                except ReferenceUnavailableException:
                    self.halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
                pickle.dump(ret, pypy_process.stdin)
            elif request == "deref_json":
                try:
                    pickle.dump({"obj": self.deref_json(**request_args), 
                                 "success": True}, pypy_process.stdin)
                except ReferenceUnavailableException:
                    self.halt_dependencies.append(request_args["ref"])
                    pickle.dump({"success": False}, pypy_process.stdin)
            elif request == "spawn":
                self.spawn_func(**request_args)
            elif request == "spawn_exec":
                self.spawn_exec_func(**request_args)
            elif request == "exec":
                try:
                    output_refs = self.exec_func(**request_args)
                    pickle.dump({"success": True, "outputs": output_refs}, pypy_process.stdin)
                except ReferenceUnavailableException:
                    pickle.dump({"success": False}, pypy_process.stdin)
                except FeatureUnavailableException as exn:
                    pickle.dump({"success": False}, pypy_process.stdin)
                    self.halt_feature_requirement = exn.feature_name
                    # The interpreter will now snapshot its own state and freeze; on resumption it will retry the exec.
            elif request == "freeze":
                # The interpreter is stopping because it needed a reference that wasn't ready yet.
                self.cont_ref = self.ref_from_pypy_dict(request_args, cont_ref_id)
                self.halt_dependencies.extend(list(request_args["additional_deps"]))
                return False
            elif request == "done":
                # The interpreter is stopping because the function has completed
                self.result_ref = self.ref_from_pypy_dict(request_args, self.expected_outputs[0])
                return True
            elif request == "exception":
                report_text = self.str_from_pypy_dict(request_args)
                raise Exception("Fatal pypy exception: %s" % report_text)

    def save_continuation(self):
        # Nah
        pass

    def add_cont_deps(self, cont_deps):
        cont_deps["_coro"] = self.cont_ref
        cont_deps["_py"] = self.pyfile_ref

    def spawn_func(self, output_id, **otherargs):

        coro_ref = self.ref_from_pypy_dict(otherargs, self.get_spawn_continuation_object_id())
        new_task_deps = {"_py": self.pyfile_ref, "_coro": coro_ref}

        self.spawn_task(new_task_deps, output_id)
        
    def deref_func(self, ref):
        cherrypy.log.error("Deref: %s" % ref.id, "SKYPY", logging.INFO)
        real_ref = self.resolve_ref(ref)
        if isinstance(real_ref, SWDataValue):
            cherrypy.log.error("Pypy dereferenced %s, returning data inline" % request_args["ref"].id,
                               "SKYPY", logging.INFO)
            return {"success": True, "strdata": data}
        else:
            cherrypy.log.error("Pypy dereferenced %s, returning %s" 
                               % (request_args["ref"].id, filename), "SKYPY", logging.INFO)
            filenames = self.block_store.retrieve_filenames_for_refs_eager([real_ref])
            return {"success": True, "filename": filenames[0]}

class SWContinuation:
    
    def __init__(self, task_stmt, context):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = context
      
    def __repr__(self):
        return "SWContinuation(task_stmt=%s, current_local_id_index=%s, stack=%s, context=%s)" % (repr(self.task_stmt), repr(self.current_local_id_index), repr(self.stack), repr(self.context))

class SafeLambdaFunction(LambdaFunction):
    
    def __init__(self, function, interpreter):
        LambdaFunction.__init__(self, function)
        self.interpreter = interpreter

    def call(self, args_list, stack, stack_base, context):
        safe_args = self.interpreter.do_eager_thunks(args_list)
        return LambdaFunction.call(self, safe_args, stack, stack_base, context)

class SWRuntimeInterpreterTask(InterpreterTask):
    
    def __init__(self, task_descriptor, block_store, execution_features, master_proxy):

        InterpreterTask.__init__(self, task_descriptor, block_store, execution_features, master_proxy)

        try:
            self.save_continuation = task_descriptor['save_continuation']
        except KeyError:
            self.save_continuation = False

        self.handler_name = "swi"
        self.lazy_derefs = set()
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
        
        cherrypy.log.error('Fetched continuation', 'SWI', logging.INFO)

    def run_interpreter(self):
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

            # XXX: This is for the unusual case that we have a task fragment that runs 
            # to completion without returning anything.
            # Could maybe use an ErrorRef here, but this might not be erroneous if, 
            # e.g. the interactive shell is used.
            if self.result is None:
                self.result = SWErrorReference('NO_RETURN_VALUE', 'null')

            self.result_ref = self.block_store.ref_from_object(self.result, "json", self.expected_outputs[0])
            return True

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
            
            self.spawned_cont_ref = self.block_store.ref_from_object(self.continuation, "pickle", self.expected_outputs[0])
            return False

    def add_cont_deps(self, cont_deps):
        cont_deps['_cont'] = self.spawned_cont_ref        

    def save_continuation(self):
        self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 
                                                              'pickle', 
                                                              self.get_saved_continuation_object_id())

    def build_spawn_continuation(self, spawn_expr, args):
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        cont = SWContinuation(spawned_task_stmt, SimpleContext())
        
        return cont

    def create_spawn_output_name(self):
        return 'swi:%s:spawnout:%d' % (self.task_id, self.spawn_counter)
    
    def spawn_func(self, spawn_expr, args):

        args = self.do_eager_thunks(args)

        # Create new continuation for the spawned function.
        spawned_continuation = self.build_spawn_continuation(spawn_expr, args)
        expected_output_id = self.create_spawn_output_name()
        spawned_cont_id = self.get_spawn_continuation_object_id(new_task_id)
        spawned_cont_ref = self.block_store.ref_from_object(spawned_continuation, 'pickle', spawned_cont_id)

        self.spawn_task(new_task_id, {"_cont": spawned_cont_ref}, expected_output_id)

        # TODO: we could visit the spawn expression and try to guess what requirements
        #       and executors we need in here. 
        # TODO: should probably look at dereference wrapper objects in the spawn context
        #       and ship them as inputs.

        # Return new future reference to the interpreter
        return SW2_FutureReference(expected_output_id)

    def do_eager_thunks(self, args):

        def resolve_thunks_mapper(leaf):
            if isinstance(leaf, SWDereferenceWrapper):
                return self.eager_dereference(leaf.ref, id_to_result_map)
            else:
                return leaf

        return map_leaf_values(resolve_thunks_mapper, args)

    def spawn_exec_func(self, executor_name, exec_args, num_outputs):

        args = self.do_eager_thunks(exec_args)

        exec_prefix = get_exec_prefix(executor_name, args, num_outputs)

        InterpreterTask.spawn_exec_func(self, executor_name, args, num_outputs, exec_prefix)

        return [SW2_FutureReference(id) for id in expected_output_ids]

    def exec_func(self, executor_name, args, num_outputs):
        
        real_args = self.do_eager_thunks(args)
        return InterpreterTask.exec_func(self, executor_name, real_args, num_outputs)

    #def make_reference(self, urls):
    #   return self.continuation.create_tasklocal_reference(SWURLReference(urls))

    def lazy_dereference(self, ref):
        self.lazy_derefs.add(ref.id)
        return SWDereferenceWrapper(ref)

    def eager_dereference(self, ref):
        # For SWI, all decodes are JSON
        try:
            ret = self.deref_json(ref)
            self.lazy_derefs.discard(ref.id)
            return ret
        except ReferenceUnavailableException:
            self.halt_dependencies.append(ref)
            raise

    def include_script(self, target_expr):
        if isinstance(target_expr, basestring):
            # Name may be relative to the local stdlib.
            target_expr = urlparse.urljoin('http://%s/stdlib/' % self.block_store.netloc, target_expr)
            target_ref = self.block_store.get_ref_for_url(target_expr, 0, self.task_id)
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

#    def select_func(self, select_group, timeout=None):
#        if self.select_result is not None:
#            return self.select_result
#        else:
#            raise SelectException(select_group, timeout)

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
