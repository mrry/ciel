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

        self.root_executor = None
        self._lock = Lock()

        self.reset()
    
    # Out-of-thread asynchronous notification calls

    def abort_task(self, task_id):
        with self._lock:
            if self.current_task_id == task_id:
                self.current_task_execution_record.abort()
            self.current_task_id = None
            self.current_task_execution_record = None

    def notify_streams_done(self, task_id):
        with self._lock:
            # Guards against changes to self.current_{task_id, task_execution_record}
            if self.root_task_id == task_id:
                # Note on threading: much like aborts, the execution_record's execute() is running
                # in another thread. It might have not yet begun, already completed, or be in progress.
                self.root_executor.notify_streams_done()
    
    # Helper functions for main

    def run_task_with_executor(self, task_descriptor, executor)
        cherrypy.engine.publish("worker_event", "Start execution " + repr(input['task_id']) + " with handler " + input['handler'])
        cherrypy.log.error("Starting task %s with handler %s" % (str(input['task_id']), new_task_handler), 'TASK', logging.INFO, False)
        try:
            executor.run(input)
            cherrypy.engine.publish("worker_event", "Completed execution " + repr(input['task_id']))
            cherrypy.log.error("Completed task %s with handler %s" % (str(input['task_id']), new_task_handler), 'TASK', logging.INFO, False)
        except:
            cherrypy.log.error("Error in task %s with handler %s" % (str(input['task_id']), new_task_handler), 'TASK', logging.ERROR, True)

    def spawn_all(self):
        if len(self.spawned_tasks) == 0:
            return
        master_proxy.spawn_tasks(self.root_task_id, self.spawned_tasks)

    def create_spawned_task_name(self):
        sha = hashlib.sha1()
        sha.update('%s:%d' % (self.task_id, self.spawn_counter))
        ret = sha.hexdigest()
        self.spawn_counter += 1
        return ret

    def commit(self):
        commit_bindings = dict([(ref.id, ref) for ref in self.published_refs])
        self.task_executor.master_proxy.commit_task(self.task_id, commit_bindings)

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
            try:
                if self.root_executor.handler != new_task_handler:
                    self.root_executor = None
            except AttributeError:
                pass
            if self.root_executor is None:
                self.root_executor = self.execution_features.get_executor(executor_name, self)
            self.root_task_id = input["task_id"]

        self.reference_cache = input["inputs"]
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
            new_task_descriptor["dependencies"] = {}
        target_executor = self.execution_features.get_executor(new_task_descriptor["handler"], self)
        # Throws a BlameUserException if we can quickly determine the task descriptor is bad
        target_executor.build_task_descriptor(new_task_descriptor, **args)
        # TODO here: use the master's task-graph apparatus.
        if "hint_small_task" in new_task_descriptor:
            for output in new_task_descriptor['expected_outputs']:
                task_for_output_id[output] = new_task_descriptor
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
        try:
            return self.resolve_ref(ref)
        except ReferenceUnavailableException as e:
            # Try running a small task to generate the required reference
            try:
                producer_task = task_for_output_id[id]
                # Presence implies hint_small_task: we should run this now
            except KeyError:
                raise e
            # Try to resolve all the child's dependencies
            try:
                producer_task["inputs"] = dict()
                for child_ref in producer_task["dependencies"]:
                    producer_task["inputs"][child_ref.id] = self.resolve_ref(child_ref)
            except ReferenceUnavailableException:
                # Child can't run now
                del producer_task["inputs"]
                raise e
            nested_executor = self.execution_features.get_executor(producer_task["handler"], self)
            self.run_task_with_executor(producer_task, nested_executor)
            # Okay the child has run, and may or may not have defined its outputs.
            # If it hasn't, this will throw appropriately
            return self.resolve_ref(ref)

# Helper function for spawn
def spawn_other(task_executor, executor_name, small_task, **executor_args):

    new_task_descriptor = {"handler": executor_name}
    if small_task:
        new_task_descriptor["hint"] = "small_task"
    task_executor.spawn_task(new_task_descriptor, **executor_args)
    return [SW2_FutureReference(ref.id) for ref in new_task_descriptor["expected_outputs"]]

class FileOrString:
    
    def __init__(self, in_dict, block_store):
        self.block_store = block_store
        if "outstr" in in_dict:
            self.str = in_dict["outstr"]
            self.filename = None
        else:
            self.str = None
            self.filename = in_dict["filename"]

    def toref(self, refid):
        if self.str is not None:
            ref = SWDataValue(refid, self.block_store.encode_datavalue(self.str))
            return ref
        else:
            _, size = self.block_store.store_file(self.filename, refid, can_move=True)
            ref = SW2_ConcreteReference(refid, size)
            ref.add_location_hint(self.block_store.netloc)
            return real_ref

    def tostr(self):
        if self.str is not None:
            return pickle.loads(self.str)
        else:
            with open(self.filename, "r") as f:
                return pickle.load(f)        

class SkyPyInterpreterTask:
    
    def __init__(self, task_executor):

        self.task_executor = task_executor
        self.skypybase = task_executor.skypybase
        self.block_store = task_executor.block_store
        
    # TODO: External spawns
    def build_task_descriptor(self, task_descriptor, coro_data, pyfile_ref):

        coro_ref = coro_data.toref("%s:coro" % task_descriptor["task_id"])
        self.task_executor.publish_reference(coro_ref)
        task_descriptor["coro_ref"] = coro_ref
        task_descriptor["dependencies"].insert(coro_ref)
        task_descriptor["py_ref"] = pyfile_ref
        task_descriptor["dependencies"].insert(pyfile_ref)
        if "expected_outputs" not in task_descriptor:
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
        
    def run(self, task_descriptor):

        halt_dependencies = []

        coroutine_ref = self.task_executor.retrieve_ref(task_descriptor["coro_ref"])
        pyfile_ref = self.task_executor.retrieve_ref(task_descriptor["py_ref"])
        rq_list = [coroutine_ref, pyfile_ref]

        filenames = block_store.retrieve_filenames_for_refs_eager(rq_list)

        coroutine_filename = filenames[0]
        py_source_filename = filenames[1]
        
        cherrypy.log.error('Fetched coroutine image to %s, .py source to %s' 
                           % (coroutine_filename, py_source_filename), 'SKYPY', logging.INFO)

        pypy_env = os.environ.copy()
        pypy_env["PYTHONPATH"] = self.skypybase + ":" + pypy_env["PYTHONPATH"]

        pypy_args = ["pypy", 
                     self.skypybase + "/stub.py", 
                     "--resume_state", coroutine_filename, 
                     "--source", py_source_filename]

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
            elif request == "spawn":
                self.spawn_func(**request_args)
            elif request == "exec":
                output_refs = spawn_other(**request_args)
                pickle.dump({"success": True, "outputs": output_refs}, pypy_process.stdin)
            elif request == "freeze":
                # The interpreter is stopping because it needed a reference that wasn't ready yet.
                coro_data = FileOrString(request_args, self.block_store)
                self.task_executor.spawn_task({"handler": "skypy", 
                                               "expected_outputs": [self.expected_output], 
                                               "dependencies": request_args["additional_deps"]},
                                              coro_data=coro_data,
                                              pyfile_ref=self.pyfile_ref)
                return False
            elif request == "done":
                # The interpreter is stopping because the function has completed
                self.task_executor.publish_reference(FileOrString(request_args, self.block_store).toref(self.expected_outputs[0]))
                return True
            elif request == "exception":
                report_text = FileOrString(request_args, self.block_store).tostr()
                raise Exception("Fatal pypy exception: %s" % report_text)

    # TODO: Use spawn-external code for this
    def spawn_func(self, **otherargs):

        new_task_descriptor = {"handler": "skypy"}
        coro_data = FileOrString(otherargs, self.block_store)
        self.task_executor.spawn_task(new_task_descriptor, coro_data=coro_data, pyfile_ref=self.pyfile_ref)
        return SW2_FutureReference(new_task_descriptor.expected_outputs[0])
        
    def deref_func(self, ref, decoder):
        cherrypy.log.error("Deref: %s" % ref.id, "SKYPY", logging.INFO)
        real_ref = self.task_executor.retrieve_ref(ref)
        if isinstance(real_ref, SWDataValue):
            return {"success": True, "strdata": self.block_store.retrieve_object_for_ref(real_ref, decoder)}
        else:
            filenames = self.block_store.retrieve_filenames_for_refs_eager([real_ref])
            return {"success": True, "filename": filenames[0]}

class SWContinuation:
    
    def __init__(self, task_stmt, context):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = context
      
    def __repr__(self):
        return "SWContinuation(task_stmt=%s, current_local_id_index=%s, stack=%s, context=%s)" % 
           (repr(self.task_stmt), repr(self.current_local_id_index), repr(self.stack), repr(self.context))

class SafeLambdaFunction(LambdaFunction):
    
    def __init__(self, function, interpreter):
        LambdaFunction.__init__(self, function)
        self.interpreter = interpreter

    def call(self, args_list, stack, stack_base, context):
        safe_args = self.interpreter.do_eager_thunks(args_list)
        return LambdaFunction.call(self, safe_args, stack, stack_base, context)

class SWRuntimeInterpreterTask:
    
    def __init__(self, task_executor):
        self.task_executor = task_executor
        self.block_store = task_executor.block_store

    def build_task_descriptor(self, task_descriptor, entry_point=None, args=None, cont=None):

        if entry_point is not None:
            # Create new continuation for the spawned function.
            # TODO: Need some reference for the appropriate SWI file.
            spawned_task_stmt = ast.Return(ast.SpawnedFunction(entry_point, args))
            cont = SWContinuation(spawned_task_stmt, SimpleContext())
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
            # TODO: we could visit the spawn expression and try to guess initial requirements 
            # TODO: should probably look at dereference wrapper objects in the spawn context
            #       and ship them as inputs.

        # else: cont is not None and this is a continuation of a previous SWI task; expected_outputs is set
        cont_id = "%s:cont" % task_descriptor["task_id"]
        spawned_cont_ref = self.block_store.ref_from_object(cont, "pickle", cont_id)
        self.task_executor.publish_reference(spawned_cont_ref)
        task_descriptor["cont"] = spawned_cont_ref
        task_descriptor["dependencies"].insert(spawned_cont_ref)

    def run(self, task_descriptor):

        try:
            save_continuation = task_descriptor["save_continuation"]
        except KeyError:
            save_continuation = False

        self.lazy_derefs = set()
        self.continuation = None
        self.result = None

        self.continuation = self.block_store.retrieve_object_for_ref(task_descriptor["cont"], 'pickle')

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
            result = visitor.visit(self.continuation.task_stmt, self.continuation.stack, 0)

            # The script finished successfully

            # XXX: This is for the unusual case that we have a task fragment that runs 
            # to completion without returning anything.
            # Could maybe use an ErrorRef here, but this might not be erroneous if, 
            # e.g. the interactive shell is used.
            if result is None:
                result = SWErrorReference('NO_RETURN_VALUE', 'null')

            result_ref = self.block_store.ref_from_object(result, "json", task_descriptor["expected_outputs"][0])
            self.task_executor.publish_reference(result_ref)
            
        except ExecutionInterruption, ei:
           
            self.task_executor.spawn_task({"handler": "swi", 
                                           "expected_outputs": [str(self.expected_output)]}, 
                                          cont=self.continuation)

        if task_descriptor["save_continuation"]:
            self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 
                                                                  'pickle', 
                                                                  "%s:saved_cont" % task_descriptor["task_id"])
            
    def spawn_func(self, spawn_expr, args):

        args = self.do_eager_thunks(args)
        new_task = self.task_executor.spawn_task({"handler": "swi"}, entry_point=spawn_expr, args=args)

        # Return new future reference to the interpreter
        return SW2_FutureReference(new_task["expected_outputs"][0])

    def do_eager_thunks(self, args):

        def resolve_thunks_mapper(leaf):
            if isinstance(leaf, SWDereferenceWrapper):
                return self.eager_dereference(leaf.ref)
            else:
                return leaf

        return map_leaf_values(resolve_thunks_mapper, args)

    def spawn_exec_func(self, executor_name, exec_args, num_outputs):
        return spawn_other(self.task_executor, executor_name, False, args=exec_args, n_outputs=num_outputs)

    def exec_func(self, executor_name, args, num_outputs):
        return spawn_other(self.task_executor, executor_name, True, args=exec_args, n_outputs=num_outputs)        

    def lazy_dereference(self, ref):
        self.lazy_derefs.add(ref)
        return SWDereferenceWrapper(ref)

    def eager_dereference(self, ref):
        # For SWI, all decodes are JSON
        real_ref = self.task_executor.retrieve_ref(ref)
        ret = self.block_store.retrieve_object_for_ref(real_ref, "json")
        self.lazy_derefs.discard(ref)
        return ret

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
