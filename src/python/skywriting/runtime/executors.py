# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#                    Chris Smowton <chris.smowton@cl.cam.ac.uk>
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

from shared.references import \
    SWRealReference, SW2_FutureReference, SW2_ConcreteReference,\
    SWDataValue, SW2_StreamReference, SWErrorReference, SW2_SweetheartReference, SW2_TombstoneReference
from skywriting.runtime.references import SWReferenceJSONEncoder
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    BlameUserException, MissingInputException

import hashlib
import urlparse
import simplejson
import logging
import shutil
import subprocess
import tempfile
import os.path
import threading
import pickle
import time
import codecs
from subprocess import PIPE
from datetime import datetime
from skywriting.runtime.block_store import STREAM_RETRY
from errno import EPIPE
from skywriting.runtime.file_watcher import get_watcher_thread

import ciel

running_children = {}

def add_running_child(proc):
    running_children[proc.pid] = proc

def remove_running_child(proc):
    del running_children[proc.pid]

def kill_all_running_children():
    for child in running_children.values():
        try:
            child.kill()
            child.wait()
        except:
            pass

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = dict([(x.handler_name, x) for x in [SkywritingExecutor, SkyPyExecutor, SWStdinoutExecutor, 
                                                           EnvironmentExecutor, JavaExecutor, DotNetExecutor, 
                                                           CExecutor, GrabURLExecutor, SyncExecutor, InitExecutor]])
        self.runnable_executors = dict([(x, self.executors[x]) for x in self.check_executors()])

    def all_features(self):
        return self.executors.keys()

    def check_executors(self):
        ciel.log.error("Checking executors:", "EXEC", logging.INFO)
        retval = []
        for (name, executor) in self.executors.items():
            if executor.can_run():
                ciel.log.error("Executor '%s' can run" % name, "EXEC", logging.INFO)
                retval.append(name)
            else:
                ciel.log.error("Executor '%s' CANNOT run" % name, "EXEC", logging.WARNING)
        return retval
    
    def can_run(self, name):
        return name in self.runnable_executors

    def get_executor(self, name, block_store):
        try:
            return self.runnable_executors[name](block_store)
        except KeyError:
            raise Exception("Can't run %s here" % name)

    def get_executor_class(self, name):
        return self.executors[name]

# Helper functions
def spawn_other(task_record, executor_name, small_task, **executor_args):

    new_task_descriptor = {"handler": executor_name}
    if small_task:
        if "task_private" not in new_task_descriptor:
            new_task_descriptor["task_private"] = dict()
        new_task_descriptor["task_private"]["hint"] = "small_task"
    task_record.spawn_task(new_task_descriptor, **executor_args)
    return [SW2_FutureReference(id) for id in new_task_descriptor["expected_outputs"]]

def package_lookup(task_record, block_store, key):
    if task_record.package_ref is None:
        ciel.log.error("Package lookup for %s in task without package" % key, "EXEC", logging.WARNING)
        return None
    package_dict = block_store.retrieve_object_for_ref(task_record.package_ref, "pickle")
    try:
        return package_dict[key]
    except KeyError:
        ciel.log.error("Package lookup for %s: no such key" % key, "EXEC", logging.WARNING)
        return None

def multi_to_single_line(s):
    lines = s.split("\n")
    lines = filter(lambda x: len(x) > 0, lines)
    s = " // ".join(lines)
    if len(s) > 100:
        s = s[:99] + "..."
    return s

def test_program(args, friendly_name):
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (outstr, errstr) = proc.communicate()
        if proc.returncode == 0:
            ciel.log.error("Successfully tested %s: wrote '%s'" % (friendly_name, multi_to_single_line(outstr)), "EXEC", logging.INFO)
            return True
        else:
            ciel.log.error("Can't run %s: returned %d, stdout: '%s', stderr: '%s'" % (friendly_name, proc.returncode, outstr, errstr), "EXEC", logging.WARNING)
            return False
    except Exception as e:
        ciel.log.error("Can't run %s: exception '%s'" % (friendly_name, e), "EXEC", logging.WARNING)
        return False

def add_package_dep(package_ref, task_descriptor):
    if package_ref is not None:
        task_descriptor["dependencies"].append(package_ref)
        task_descriptor["task_private"]["package_ref"] = package_ref

def hash_update_with_structure(hash, value):
    """
    Recurses over a Skywriting data structure (containing lists, dicts and 
    primitive leaves) in a deterministic order, and updates the given hash with
    all values contained therein.
    """
    if isinstance(value, list):
        hash.update('[')
        for element in value:
            hash_update_with_structure(hash, element)
            hash.update(',')
        hash.update(']')
    elif isinstance(value, dict):
        hash.update('{')
        for (dict_key, dict_value) in sorted(value.items()):
            hash.update(dict_key)
            hash.update(':')
            hash_update_with_structure(hash, dict_value)
            hash.update(',')
        hash.update('}')
    elif isinstance(value, SWRealReference):
        hash.update('ref')
        hash.update(value.id)
    else:
        hash.update(str(value))

def map_leaf_values(f, value):
    """
    Recurses over a data structure containing lists, dicts and primitive leaves), 
    and returns a new structure with the leaves mapped as specified.
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

# Helper class for SkyPy
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
            ref = self.block_store.ref_from_string(self.str, refid)
        else:
            ref = self.block_store.ref_from_external_file(self.filename, refid)
        return ref

    def tostr(self):
        if self.str is not None:
            return pickle.loads(self.str)
        else:
            with open(self.filename, "r") as f:
                return pickle.load(f)

    def toobj(self):
        return self.tostr()

class SkyPyExecutor:

    handler_name = "skypy"
    
    def __init__(self, block_store):
        self.block_store = block_store
        self.skypybase = os.getenv("CIEL_SKYPY_BASE")

    def cleanup(self):
        pass
        
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, pyfile_ref=None, coro_data=None, entry_point=None, entry_args=None, export_json=False):

        if pyfile_ref is None:
            raise BlameUserException("All SkyPy invocations must specify a .py file reference as 'pyfile_ref'")
        if coro_data is not None:
            if "task_id" not in task_descriptor:
                raise Exception("Can't spawn SkyPy tasks from coroutines without a task id")
            coro_ref = coro_data.toref("%s:coro" % task_descriptor["task_id"])
            parent_task_record.publish_ref(coro_ref)
            task_descriptor["task_private"]["coro_ref"] = coro_ref
            task_descriptor["dependencies"].append(coro_ref)
        else:
            task_descriptor["task_private"]["entry_point"] = entry_point
            task_descriptor["task_private"]["entry_args"] = entry_args
        task_descriptor["task_private"]["py_ref"] = pyfile_ref
        task_descriptor["dependencies"].append(pyfile_ref)
        task_descriptor["task_private"]["export_json"] = export_json
        if "expected_outputs" not in task_descriptor and "task_id" in task_descriptor:
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
        add_package_dep(parent_task_record.package_ref, task_descriptor)

    @staticmethod
    def can_run():
        if "CIEL_SKYPY_BASE" not in os.environ:
            ciel.log.error("Can't run SkyPy: CIEL_SKYPY_BASE not in environment", "SKYPY", logging.WARNING)
            return False
        else:
            return test_program(["pypy", os.getenv("CIEL_SKYPY_BASE") + "/stub.py", "--version"], "PyPy")
        
    def run(self, task_descriptor, task_record):

        self.task_descriptor = task_descriptor
        self.task_record = task_record
        halt_dependencies = []
        skypy_private = task_descriptor["task_private"]

        pyfile_ref = self.task_record.retrieve_ref(skypy_private["py_ref"])
        self.pyfile_ref = pyfile_ref
        rq_list = [pyfile_ref]
        if "coro_ref" in skypy_private:
            coroutine_ref = self.task_record.retrieve_ref(skypy_private["coro_ref"])
            rq_list.append(coroutine_ref)

        filenames = self.block_store.retrieve_filenames_for_refs(rq_list)

        py_source_filename = filenames[0]
        if "coro_ref" in skypy_private:
            coroutine_filename = filenames[1]
            ciel.log.error('Fetched coroutine image to %s, .py source to %s' 
                               % (coroutine_filename, py_source_filename), 'SKYPY', logging.INFO)
        else:
            ciel.log.error("Fetched .py source to %s; starting from entry point %s, args %s"
                               % (py_source_filename, skypy_private["entry_point"], skypy_private["entry_args"]))

        pypy_env = os.environ.copy()
        pypy_env["PYTHONPATH"] = self.skypybase + ":" + pypy_env["PYTHONPATH"]

        pypy_args = ["pypy", 
                     self.skypybase + "/stub.py", 
                     "--source", py_source_filename]

        if "coro_ref" in skypy_private:
            pypy_args.extend(["--resume_state", coroutine_filename])
        else:
            pypy_args.append("--await_entry_point")
            
        pypy_process = subprocess.Popen(pypy_args, env=pypy_env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        if "coro_ref" not in skypy_private:
            pickle.dump({"entry_point": skypy_private["entry_point"], "entry_args": skypy_private["entry_args"]}, pypy_process.stdin)

        while True:
            
            request_args = pickle.load(pypy_process.stdout)
            request = request_args["request"]
            del request_args["request"]
            ciel.log.error("Request: %s" % request, "SKYPY", logging.DEBUG)
            # The key difference between deref and deref_json is that the JSON variant MUST be decoded locally
            # This is a hack around the fact that the simplejson library hasn't been ported to pypy.
            if request == "deref":
                try:
                    ret = self.deref_func(**request_args)
                except ReferenceUnavailableException:
                    halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
                pickle.dump(ret, pypy_process.stdin)
            elif request == "deref_json":
                try:
                    ret = self.deref_json(**request_args)
                except ReferenceUnavailableException:
                    halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
                pickle.dump(ret, pypy_process.stdin)                
            elif request == "spawn":
                out_ref = self.spawn_func(**request_args)
                pickle.dump({"output": out_ref}, pypy_process.stdin)
            elif request == "exec":
                out_refs = spawn_other(self.task_record, **request_args)
                pickle.dump({"outputs": out_refs}, pypy_process.stdin)
            elif request == "package_lookup":
                pickle.dump({"value": package_lookup(self.task_record, self.block_store, request_args["key"])}, pypy_process.stdin)
            elif request == "freeze":
                # The interpreter is stopping because it needed a reference that wasn't ready yet.
                coro_data = FileOrString(request_args, self.block_store)
                cont_deps = halt_dependencies
                cont_deps.extend(request_args["additional_deps"])
                self.task_record.spawn_task({"handler": "skypy",
                                               "expected_outputs": task_descriptor["expected_outputs"], 
                                               "dependencies": cont_deps,
                                               "task_private": {"hint": "small_task"}},
                                              coro_data=coro_data,
                                              pyfile_ref=pyfile_ref,
                                              export_json=skypy_private["export_json"])
                return
            elif request == "done":
                # The interpreter is stopping because the function has completed
                result = FileOrString(request_args, self.block_store)
                if skypy_private["export_json"]:
                    result_ref = self.block_store.ref_from_object(result.toobj(), "json", task_descriptor["expected_outputs"][0])
                else:
                    result_ref = result.toref(task_descriptor["expected_outputs"][0])
                self.task_record.publish_ref(result_ref)
                return
            elif request == "exception":
                report_text = FileOrString(request_args, self.block_store).tostr()
                raise Exception("Fatal pypy exception: %s" % report_text)
            else:
                raise Exception("Pypy requested bad operation: %s / %s" % (request, request_args))

    # Note this is not the same as an external spawn -- it could e.g. spawn an anonymous lambda
    def spawn_func(self, **otherargs):

        new_task_descriptor = {"handler": "skypy"}
        coro_data = FileOrString(otherargs, self.block_store)
        self.task_record.spawn_task(new_task_descriptor, coro_data=coro_data, pyfile_ref=self.pyfile_ref)
        return SW2_FutureReference(new_task_descriptor["expected_outputs"][0])
        
    def deref_func(self, ref):
        ciel.log.error("Deref: %s" % ref.id, "SKYPY", logging.INFO)
        real_ref = self.task_record.retrieve_ref(ref)
        if isinstance(real_ref, SWDataValue):
            return {"success": True, "strdata": self.block_store.retrieve_object_for_ref(real_ref, "noop")}
        else:
            filenames = self.block_store.retrieve_filenames_for_refs([real_ref])
            return {"success": True, "filename": filenames[0]}

    def deref_json(self, ref):
        real_ref = self.task_record.retrieve_ref(ref)
        return {"success": True, "obj": self.block_store.retrieve_object_for_ref(ref, "json")}

# Imports for Skywriting

from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    BlameUserException, MissingInputException, ExecutionInterruption
from skywriting.lang.context import SimpleContext, TaskContext,\
    LambdaFunction
from skywriting.lang.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from skywriting.lang import ast
from skywriting.lang.parser import \
    SWScriptParser

# Helpers for Skywriting

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

class SkywritingExecutor:

    handler_name = "swi"

    def __init__(self, block_store):
        self.block_store = block_store
        self.stdlibbase = os.getenv("CIEL_SW_STDLIB", None)

    def cleanup(self):
        pass

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, sw_file_ref=None, start_env=None, start_args=None, cont=None):

        if "expected_outputs" not in task_descriptor and "task_id" in task_descriptor:
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
        if cont is not None:
            cont_id = "%s:cont" % task_descriptor["task_id"]
            spawned_cont_ref = block_store.ref_from_object(cont, "pickle", cont_id)
            parent_task_record.publish_ref(spawned_cont_ref)
            task_descriptor["task_private"]["cont"] = spawned_cont_ref
            task_descriptor["dependencies"].append(spawned_cont_ref)
        else:
            # External call: SW file should be started from the beginning.
            task_descriptor["task_private"]["swfile_ref"] = sw_file_ref
            task_descriptor["dependencies"].append(sw_file_ref)
            task_descriptor["task_private"]["start_env"] = start_env
            task_descriptor["task_private"]["start_args"] = start_args
        add_package_dep(parent_task_record.package_ref, task_descriptor)

    def start_sw_script(self, swref, args, env):

        sw_file = self.block_store.retrieve_filename_for_ref(swref)
        parser = SWScriptParser()
        with open(sw_file, "r") as sw_fp:
            script = parser.parse(sw_fp.read())

        if script is None:
            raise Exception("Couldn't parse %s" % swref)
    
        cont = SWContinuation(script, SimpleContext())
        if env is not None:
            cont.context.bind_identifier('env', env)
        if args is not None:
            cont.context.bind_identifier('argv', args)
        return cont

    @staticmethod
    def can_run():
        return True

    def run(self, task_descriptor, task_record):

        sw_private = task_descriptor["task_private"]
        self.task_id = task_descriptor["task_id"]
        self.task_record = task_record

        try:
            save_continuation = task_descriptor["save_continuation"]
        except KeyError:
            save_continuation = False

        self.lazy_derefs = set()
        self.continuation = None
        self.result = None

        if "cont" in sw_private:
            self.continuation = self.block_store.retrieve_object_for_ref(sw_private["cont"], 'pickle')
        else:
            self.continuation = self.start_sw_script(sw_private["swfile_ref"], sw_private["start_args"], sw_private["start_env"])

        self.continuation.context.restart()
        task_context = TaskContext(self.continuation.context, self)
        
        task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.spawn_func(x[0], x[1])))
        task_context.bind_tasklocal_identifier("spawn_exec", LambdaFunction(lambda x: self.spawn_exec_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("spawn_other", LambdaFunction(lambda x: self.spawn_other(x[0], x[1])))
        task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
        task_context.bind_tasklocal_identifier("int", SafeLambdaFunction(lambda x: int(x[0]), self))
        task_context.bind_tasklocal_identifier("range", SafeLambdaFunction(lambda x: range(*x), self))
        task_context.bind_tasklocal_identifier("len", SafeLambdaFunction(lambda x: len(x[0]), self))
        task_context.bind_tasklocal_identifier("has_key", SafeLambdaFunction(lambda x: x[1] in x[0], self))
        task_context.bind_tasklocal_identifier("get_key", SafeLambdaFunction(lambda x: x[0][x[1]] if x[1] in x[0] else x[2], self))
        task_context.bind_tasklocal_identifier("exec", LambdaFunction(lambda x: self.exec_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("package", LambdaFunction(lambda x: package_lookup(self.task_record, self.block_store, x[0])))

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
            self.task_record.publish_ref(result_ref)
            
        except ExecutionInterruption, ei:
           
            self.task_record.spawn_task({"handler": "swi", 
                                           "expected_outputs": task_descriptor["expected_outputs"],
                                           "dependencies": list(self.lazy_derefs),
                                           "task_private": { "hint": "small_task" }},
                                          cont=self.continuation)

# TODO: Fix this?
#        if "save_continuation" in task_descriptor and task_descriptor["save_continuation"]:
#            self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 
#                                                                  'pickle', 
#                                                                  "%s:saved_cont" % task_descriptor["task_id"])
            
    def spawn_func(self, spawn_expr, args):

        args = self.do_eager_thunks(args)
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        cont = SWContinuation(spawned_task_stmt, SimpleContext())
        new_task = self.task_record.spawn_task({"handler": "swi"}, cont=cont)

        # Return new future reference to the interpreter
        return SW2_FutureReference(new_task["expected_outputs"][0])

    def do_eager_thunks(self, args):

        def resolve_thunks_mapper(leaf):
            if isinstance(leaf, SWDereferenceWrapper):
                return self.eager_dereference(leaf.ref)
            else:
                return leaf

        return map_leaf_values(resolve_thunks_mapper, args)

    def spawn_other(self, executor_name, executor_args_dict):
        # Args dict arrives from sw with unicode keys :(
        str_args = dict([(str(k), v) for (k, v) in executor_args_dict.items()])
        try:
            small_task = str_args.pop("small_task")
        except:
            small_task = False
        return spawn_other(self.task_record, executor_name, small_task, **str_args)

    def spawn_exec_func(self, executor_name, args, num_outputs):
        return spawn_other(self.task_record, executor_name, False, args=args, n_outputs=num_outputs)

    def exec_func(self, executor_name, args, num_outputs):
        return spawn_other(self.task_record, executor_name, True, args=args, n_outputs=num_outputs)        

    def lazy_dereference(self, ref):
        self.lazy_derefs.add(ref)
        return SWDereferenceWrapper(ref)

    def eager_dereference(self, ref):
        # For SWI, all decodes are JSON
        real_ref = self.task_record.retrieve_ref(ref)
        ret = self.block_store.retrieve_object_for_ref(real_ref, "json")
        self.lazy_derefs.discard(ref)
        return ret

    def include_script(self, target_expr):
        if isinstance(target_expr, basestring):
            # Name may be relative to the local stdlib.
            if not target_expr.startswith('http'):
                target_url = 'file://%s' % os.path.join(self.stdlibbase, target_expr)
            else:
                target_url = target_expr
            target_ref = self.block_store.get_ref_for_url(target_url, 0, self.task_id)
        elif isinstance(target_expr, SWRealReference):    
            target_ref = target_expr
        else:
            raise BlameUserException('Invalid object %s passed as the argument of include', 'INCLUDE', logging.ERROR)

        try:
            script = self.block_store.retrieve_object_for_ref(target_ref, 'script')
        except:
            ciel.log.error('Error parsing included script', 'INCLUDE', logging.ERROR, True)
            raise BlameUserException('The included script did not parse successfully')
        return script

class SimpleExecutor:

    def __init__(self, block_store):
        self.block_store = block_store

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, args, n_outputs):

        # Throw early if the args are bad
        cls.check_args_valid(args, n_outputs)

        # Discover required ref IDs for this executor
        reqd_refs = cls.get_required_refs(args)
        task_descriptor["dependencies"].extend(reqd_refs)

        sha = hashlib.sha1()
        hash_update_with_structure(sha, [args, n_outputs])
        name_prefix = "%s:%s:" % (cls.handler_name, sha.hexdigest())

        # Name our outputs
        if "expected_outputs" not in task_descriptor:
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]

        # Add the args dict
        args_name = "%ssimple_exec_args" % name_prefix
        args_ref = block_store.ref_from_object(args, "pickle", args_name)
        parent_task_record.publish_ref(args_ref)
        task_descriptor["dependencies"].append(args_ref)
        task_descriptor["task_private"]["simple_exec_args"] = args_ref
        
    def resolve_required_refs(self, args):
        try:
            args["inputs"] = [self.task_record.retrieve_ref(ref) for ref in args["inputs"]]
        except KeyError:
            pass

    @classmethod
    def get_required_refs(cls, args):
        try:
            # Shallow copy
            return list(args["inputs"])
        except KeyError:
            return []

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "inputs" in args:
            for ref in args["inputs"]:
                if not isinstance(ref, SWRealReference):
                    raise BlameUserException("Simple executors need args['inputs'] to be a list of references. %s is not a reference." % ref)

    @staticmethod
    def can_run():
        return True

    def run(self, task_descriptor, task_record):
        self.task_record = task_record
        self.task_id = task_descriptor["task_id"]
        self.output_ids = task_descriptor["expected_outputs"]
        self.output_refs = [None for i in range(len(self.output_ids))]
        self.succeeded = False
        self.args = self.block_store.retrieve_object_for_ref(task_descriptor["task_private"]["simple_exec_args"], "pickle")

        try:
            self.debug_opts = self.args['debug_options']
        except KeyError:
            self.debug_opts = []
        self.resolve_required_refs(self.args)
        try:
            self._execute()
            for ref in self.output_refs:
                if ref is not None:
                    self.task_record.publish_ref(ref)
                else:
                    ciel.log.error("Executor failed to define output %s" % ref.id, "EXEC", logging.WARNING)
            self.succeeded = True
        except:
            ciel.log.error("Task execution failed", "EXEC", logging.ERROR, True)
            raise
        finally:
            self.cleanup_task()
        
    def cleanup_task(self):
        self._cleanup_task()
    
    def _cleanup_task(self):
        pass

    def cleanup(self):
        pass
    
    def abort(self):
        self._abort()
        
    def _abort(self):
        pass

class AsyncPushThread:

    def __init__(self, block_store, ref, fifos_dir):
        self.block_store = block_store
        self.ref = ref
        self.fifos_dir = fifos_dir
        self.success = None
        self.lock = threading.Lock()
        self.fetch_done = False
        self.stream_done = False
        self.bytes_copied = 0
        self.bytes_available = 0
        self.next_threshold = 0
        self.condvar = threading.Condition(self.lock)
        self.thread = None
        self.filename = None
        self.file_fetch = block_store.fetch_ref_async(ref, 
                                                      result_callback=self.result, 
                                                      reset_callback=self.reset, 
                                                      progress_callback=self.progress)
        if not self.fetch_done:
            ciel.log("Fetch for %s did not complete immediately; creating push thread" % ref, "EXEC", logging.INFO)
            self.thread = threading.Thread(target=self.copy_loop)
            self.thread.start()
        else:
            ciel.log("Fetch for %s completed immediately" % ref, "EXEC", logging.INFO)
            with self.lock:
                self.filename = self.file_fetch.get_filename()
                self.stream_done = True
                self.condvar.notify_all()

    def copy_loop(self):
        
        try:
            # Do this here to avoid blocking in constructor
            read_filename = self.file_fetch.get_filename()
            with self.lock:
                if self.fetch_done:
                    self.filename = os.path.join(self.fifos_dir, "fifo-%s" % self.ref.id)
                    ciel.log("Fetch for %s not yet complete; pushing through FIFO %s" % (self.ref, self.filename), "EXEC", logging.INFO)   
                    os.mkfifo(self.filename)
                else:
                    ciel.log("Fetch for %s completed during get_filename; reading directly" % self.ref, "EXEC", logging.INFO)
                    self.stream_done = True
                    self.filename = read_filename
                self.condvar.notify_all()
                if self.fetch_done:
                    return
            with open(read_filename, "r") as input_fp:
                with open(self.filename, "w") as output_fp:
                    while True:
                        while True:
                            buf = input_fp.read(4096)
                            output_fp.write(buf)
                            self.bytes_copied += len(buf)
                            with self.lock:
                                if self.bytes_copied == self.bytes_available and self.fetch_done:
                                    self.stream_done = True
                                    self.condvar.notify_all()
                                    return
                            if len(buf) < 4096:
                                # EOF, for now.
                                break
                        with self.lock:
                            self.next_threshold = self.bytes_copied + 67108864
                            while self.bytes_available < self.next_threshold and not self.fetch_done:
                                self.condvar.wait()
        except Exception as e:
            ciel.log("Push thread for %s died with exception %s" % (read_filename, e), "EXEC", logging.WARNING)
            with self.lock:
                self.stream_done = True
                self.condvar.notify_all()

    def result(self, success):
        if success:
            completed = "failed!"
        else:
            completed = "completed"
        if self.thread is None:
            completed = "completed without transfer"
            prefix = "Fetch"
        else:
            prefix = "Asynchronous fetch"
            ciel.log("%s of ref %s %s" % (prefix, self.ref, completed), "EXEC", logging.INFO)

        with self.lock:
            self.success = success
            self.fetch_done = True
            self.condvar.notify_all()

    def progress(self, bytes_downloaded):
        with self.lock:
            self.bytes_available = bytes_downloaded
            if self.bytes_available >= self.next_threshold:
                self.condvar.notify_all()

    def reset(self):
        ciel.log("Reset of streamed fetch for %s!" % self.ref, "EXEC", logging.WARNING)
        self.result(False)

    def get_filename(self):
        with self.lock:
            while self.filename is None:
                self.condvar.wait()
        return self.filename

    def wait(self):
        with self.lock:
            while not self.stream_done:
                self.condvar.wait()

class ProcessRunningExecutor(SimpleExecutor):

    def __init__(self, block_store):
        SimpleExecutor.__init__(self, block_store)

        self._lock = threading.Lock()
        self.proc = None
        self.outputs_in_progress = False
        self.output_subscriptions = []
        self.file_watcher_thread = get_watcher_thread()

    def _execute(self):
        try:
            self.input_refs = self.args['inputs']
        except KeyError:
            self.input_refs = []
        try:
            self.stream_output = self.args['stream_output']
        except KeyError:
            self.stream_output = False
        try:
            self.eager_fetch = self.args['eager_fetch']
        except KeyError:
            self.eager_fetch = False

        try:
            self.make_sweetheart = self.args['make_sweetheart']
            if not isinstance(self.make_sweetheart, list):
                self.make_sweetheart = [self.make_sweetheart]
        except KeyError:
            self.make_sweetheart = []

        if self.eager_fetch:
            push_threads = None
            file_inputs = self.block_store.retrieve_filenames_for_refs(self.input_refs)
        else:
            fifos_dir = tempfile.mkdtemp(prefix="ciel-fetch-fifos-")
            push_threads = [AsyncPushThread(self.block_store, ref, fifos_dir) for ref in self.input_refs]
            file_inputs = [push_thread.get_filename() for push_thread in push_threads]

        with self._lock:
            out_file_contexts = [self.block_store.make_local_output(id, self) for id in self.output_ids]
            self.outputs_in_progress = True

        file_outputs = [ctx.get_filename() for ctx in out_file_contexts]
        
        if self.stream_output:
           
            stream_refs = [ctx.get_stream_ref() for ctx in out_file_contexts]
            to_publish = dict([(ref.id, ref) for ref in stream_refs])
            self.task_record.prepublish_refs(to_publish)

        self.proc = self.start_process(file_inputs, file_outputs)
        add_running_child(self.proc)

        rc = self.await_process(file_inputs, file_outputs)
        remove_running_child(self.proc)

        self.proc = None

#        if "trace_io" in self.debug_opts:
#            transfer_ctx.log_traces()

        # Wait for the threads pushing input to the client to finish. Could cancel or detach here instead.
        if push_threads is not None:
            ciel.log("Waiting for push threads to complete", "EXEC", logging.INFO)
            for thread in push_threads:
                thread.wait()
            shutil.rmtree(fifos_dir)

        # If we have fetched any objects to this worker, publish them at the master.
        # TODO: Do this cleanly by using context objects returned by the BlockStor
        ciel.log("Publishing fetched references", "EXEC", logging.INFO)
        extra_publishes = {}
        for ref in self.input_refs:
            if isinstance(ref, SW2_ConcreteReference) and not self.block_store.netloc in ref.location_hints:
                extra_publishes[ref.id] = SW2_ConcreteReference(ref.id, ref.size_hint, [self.block_store.netloc])
        for sweetheart in self.make_sweetheart:
            extra_publishes[sweetheart.id] = SW2_SweetheartReference(sweetheart.id, sweetheart.size_hint, self.block_store.netloc, [self.block_store.netloc])
        if len(extra_publishes) > 0:
            self.task_record.prepublish_refs(extra_publishes)

        if push_threads is not None:
            failed_threads = filter(lambda t: not t.success, push_threads)
            failure_bindings = dict([(ft.ref.id, SW2_TombstoneReference(ft.ref.id, ft.ref.location_hints)) for ft in failed_threads])
            if len(failure_bindings) > 0:
                raise MissingInputException(failure_bindings)

        if rc != 0:
            for output in out_file_contexts:
                output.rollback()
            raise OSError()

        ciel.engine.publish("worker_event", "Executor: Storing outputs")
        ciel.log("Publishing created references", "EXEC", logging.INFO)

        for i, output in enumerate(out_file_contexts):
            output.close()
            self.output_refs[i] = output.get_completed_ref()

        with self._lock:
            for watch in self.output_subscriptions:
                watch.file_done()
            self.output_subscriptions = []
            self.outputs_in_progress = False

        ciel.engine.publish("worker_event", "Executor: Done")

    def subscribe_output(self, otherend_netloc, output_id):
        with self._lock:
            if self.outputs_in_progress:
                watch = self.file_watcher_thread.add_watch(otherend_netloc, output_id)
                self.output_subscriptions.add(watch)
            else:
                raise Exception("Executor has finished (or not yet begun, in which case how did you find this StreamRef, eh?)")

    def start_process(self, input_files, output_files):
        raise Exception("Must override start_process when subclassing ProcessRunningExecutor")
        
    def await_process(self, input_files, output_files):
        rc = self.proc.wait()
        return rc

    def _cleanup_task(self):
        if self.stream_output and not self.succeeded:
            self.block_store.rollback_file(self.output_ids[0])
        
    def _abort(self):
        if self.proc is not None:
            self.proc.kill()

class SWStdinoutExecutor(ProcessRunningExecutor):
    
    handler_name = "stdinout"

    def __init__(self, block_store):
        ProcessRunningExecutor.__init__(self, block_store)

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        ProcessRunningExecutor.check_args_valid(args, n_outputs)
        if n_outputs != 1:
            raise BlameUserException("Stdinout executor must have one output")
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))

    def start_process(self, input_files, output_files):

        command_line = self.args["command_line"]
        ciel.log.error("Executing stdinout with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.INFO)

        with open(output_files[0], "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            return subprocess.Popen(map(str, command_line), stdin=PIPE, stdout=temp_output_fp, close_fds=True)

    def await_process(self, input_files, output_files):

        class list_with:
            def __init__(self, l):
                self.wrapped_list = l
            def __enter__(self):
                return [x.__enter__() for x in self.wrapped_list]
            def __exit__(self, exnt, exnv, exntb):
                for x in self.wrapped_list:
                    x.__exit__(exnt, exnv, exntb)
                return False

        with list_with([open(filename, 'r') for filename in input_files]) as fileobjs:
            for fileobj in fileobjs:
                try:
                    shutil.copyfileobj(fileobj, self.proc.stdin)
                except IOError, e:
                    if e.errno == EPIPE:
                        ciel.log.error('Abandoning cat due to EPIPE', 'EXEC', logging.WARNING)
                        break
                    else:
                        raise

        self.proc.stdin.close()
        rc = self.proc.wait()
        return rc
        
class EnvironmentExecutor(ProcessRunningExecutor):

    handler_name = "env"

    def __init__(self, block_store):
        ProcessRunningExecutor.__init__(self, block_store)

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        ProcessRunningExecutor.check_args_valid(args, n_outputs)
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))

    def start_process(self, input_files, output_files):

        command_line = self.args["command_line"]
        ciel.log.error("Executing environ with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.INFO)

        with tempfile.NamedTemporaryFile(delete=False) as input_filenames_file:
            for filename in input_files:
                input_filenames_file.write(filename)
                input_filenames_file.write('\n')
            input_filenames_name = input_filenames_file.name
            
        with tempfile.NamedTemporaryFile(delete=False) as output_filenames_file:
            for filename in output_files:
                output_filenames_file.write(filename)
                output_filenames_file.write('\n')
            output_filenames_name = output_filenames_file.name
            
        environment = {'INPUT_FILES'  : input_filenames_name,
                       'OUTPUT_FILES' : output_filenames_name}
            
        proc = subprocess.Popen(map(str, command_line), env=environment, close_fds=True)

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        return proc

class FilenamesOnStdinExecutor(ProcessRunningExecutor):
    
    def __init__(self, block_store):
        ProcessRunningExecutor.__init__(self, block_store)

        self.last_event_time = None
        self.current_state = "Starting up"
        self.state_times = dict()

    def change_state(self, new_state):
        time_now = datetime.now()
        old_state_time = time_now - self.last_event_time
        old_state_secs = float(old_state_time.seconds) + (float(old_state_time.microseconds) / 10**6)
        if self.current_state not in self.state_times:
            self.state_times[self.current_state] = old_state_secs
        else:
            self.state_times[self.current_state] += old_state_secs
        self.last_event_time = time_now
        self.current_state = new_state

    def resolve_required_refs(self, args):
        SimpleExecutor.resolve_required_refs(self, args)
        try:
            args["lib"] = [self.task_record.retrieve_ref(ref) for ref in args["lib"]]
        except KeyError:
            pass

    @classmethod
    def get_required_refs(cls, args):
        l = SimpleExecutor.get_required_refs(args)
        try:
            l.extend(args["lib"])
        except KeyError:
            pass
        return l

    def start_process(self, input_files, output_files):

        try:
            self.argv = self.args['argv']
        except KeyError:
            self.argv = []

        self.before_execute()
        ciel.engine.publish("worker_event", "Executor: running")

        if "go_slow" in self.debug_opts:
            ciel.log.error("DEBUG: Executor sleep(3)'ing", "EXEC", logging.DEBUG)
            time.sleep(3)

        proc = subprocess.Popen(self.get_process_args(), shell=False, stdin=PIPE, stdout=PIPE, stderr=None, close_fds=True)
        self.last_event_time = datetime.now()
        self.change_state("Writing input details")
        
        proc.stdin.write("%d,%d,%d\0" % (len(input_files), len(output_files), len(self.argv)))
        for x in input_files:
            proc.stdin.write("%s\0" % x)
        for x in output_files:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()
        self.change_state("Waiting for FIFO pickup")

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        return proc

    def gather_io_trace(self):
        anything_read = False
        while True:
            try:
                message = ""
                while True:
                    c = self.proc.stdout.read(1)
                    if not anything_read:
                        self.change_state("Gathering IO trace")
                        anything_read = True
                    if c == ",":
                        if message[0] == "C":
                           timestamp = float(message[1:])
                           ciel.engine.publish("worker_event", "Process log %f Computing" % timestamp)
                        elif message[0] == "I":
                            try:
                                params = message[1:].split("|")
                                stream_id = int(params[0])
                                timestamp = float(params[1])
                                ciel.engine.publish("worker_event", "Process log %f Waiting %d" % (timestamp, stream_id))
                            except:
                                ciel.log.error("Malformed data from stdout: %s" % message)
                                raise
                        else:
                            ciel.log.error("Malformed data from stdout: %s" % message)
                            raise Exception("Malformed stuff")
                        break
                    elif c == "":
                        raise Exception("Stdout closed")
                    else:
                        message = message + c
            except Exception as e:
                print e
                break

    def await_process(self, input_files, output_files):
        self.change_state("Running")
        if "trace_io" in self.debug_opts:
            ciel.log.error("DEBUG: Executor gathering an I/O trace from child", "EXEC", logging.INFO)
            self.gather_io_trace()
        rc = self.proc.wait()
        self.change_state("Done")
        ciel.log.error("Process terminated. Stats:", "EXEC", logging.INFO)
        for key, value in self.state_times.items():
            ciel.log.error("Time in state %s: %s seconds" % (key, value), "EXEC", logging.INFO)
        return rc

    def get_process_args(self):
        raise Exception("Must override get_process_args subclassing FilenamesOnStdinExecutor")

class JavaExecutor(FilenamesOnStdinExecutor):

    handler_name = "java"

    def __init__(self, block_store):
        FilenamesOnStdinExecutor.__init__(self, block_store)

    @staticmethod
    def can_run():
        cp = os.getenv("CLASSPATH")
        if cp is None:
            ciel.log.error("Can't run Java: no CLASSPATH set", "JAVA", logging.WARNING)
            return False
        else:
            return test_program(["java", "-cp", cp, "uk.co.mrry.mercator.task.JarTaskLoader", "--version"], "Java")

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        FilenamesOnStdinExecutor.check_args_valid(args, n_outputs)
        if "lib" not in args or "class" not in args:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))

    def before_execute(self):

        self.jar_refs = self.args["lib"]
        self.class_name = self.args["class"]

        ciel.log.error("Running Java executor for class: %s" % self.class_name, "JAVA", logging.INFO)
        ciel.engine.publish("worker_event", "Java: fetching JAR")

        self.jar_filenames = self.block_store.retrieve_filenames_for_refs(self.jar_refs)

    def get_process_args(self):
        cp = os.getenv('CLASSPATH')
        process_args = ["java", "-cp", cp]
        if "trace_io" in self.debug_opts:
            process_args.append("-Dskywriting.trace_io=1")
        process_args.extend(["uk.co.mrry.mercator.task.JarTaskLoader", self.class_name])
        process_args.extend(["file://" + x for x in self.jar_filenames])
        return process_args
        
class DotNetExecutor(FilenamesOnStdinExecutor):

    handler_name = "dotnet"

    def __init__(self, block_store):
        FilenamesOnStdinExecutor.__init__(self, block_store)

    @staticmethod
    def can_run():
        mono_loader = os.getenv('SW_MONO_LOADER_PATH')
        if mono_loader is None:
            ciel.log.error("Can't run Mono: SW_MONO_LOADER_PATH not set", "DOTNET", logging.WARNING)
            return False
        return test_program(["mono", mono_loader, "--version"], "Mono")

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        FilenamesOnStdinExecutor.check_args_valid(args, n_outputs)
        if "lib" not in args or "class" not in args:
            raise BlameUserException('Incorrect arguments to the dotnet executor: %s' % repr(args))

    def before_execute(self):

        self.dll_refs = self.args['lib']
        self.class_name = self.args['class']

        ciel.log.error("Running Dotnet executor for class: %s" % self.class_name, "DOTNET", logging.INFO)
        ciel.engine.publish("worker_event", "Dotnet: fetching DLLs")
        self.dll_filenames = self.block_store.retrieve_filenames_for_refs(self.dll_refs)

    def get_process_args(self):

        mono_loader = os.getenv('SW_MONO_LOADER_PATH')
        process_args = ["mono", mono_loader, self.class_name]
        process_args.extend(self.dll_filenames)
        return process_args

class CExecutor(FilenamesOnStdinExecutor):

    handler_name = "cso"

    def __init__(self, block_store):
        FilenamesOnStdinExecutor.__init__(self, block_store)

    @staticmethod
    def can_run():
        c_loader = os.getenv("SW_C_LOADER_PATH")
        if c_loader is None:
            ciel.log.error("Can't run C tasks: SW_C_LOADER_PATH not set", "CEXEC", logging.WARNING)
            return False
        return test_program([c_loader, "--version"], "C-loader")

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        FilenamesOnStdinExecutor.check_args_valid(args, n_outputs)
        if "lib" not in args or "entry_point" not in args:
            raise BlameUserException('Incorrect arguments to the C-so executor: %s' % repr(args))

    def before_execute(self, block_store):
        self.so_refs = self.args['lib']
        self.entry_point_name = self.args['entry_point']

        ciel.log.error("Running C executor for entry point: %s" % self.entry_point_name, "CEXEC", logging.INFO)
        ciel.engine.publish("worker_event", "C-exec: fetching SOs")
        self.so_filenames = self.retrieve_filenames_for_refs(self.so_refs)

    def get_process_args(self):

        c_loader = os.getenv('SW_C_LOADER_PATH')
        process_args = [c_loader, self.entry_point_name]
        process_args.extend(self.so_filenames)
        return process_args
    
class GrabURLExecutor(SimpleExecutor):

    handler_name = "grab"
    
    def __init__(self, block_store):
        SimpleExecutor.__init__(self, block_store)
    
    @classmethod
    def check_args_valid(cls, args, n_outputs):
        
        SimpleExecutor.check_args_valid(args, n_outputs)
        if "urls" not in args or "version" not in args or len(args["urls"]) != n_outputs:
            raise BlameUserException('Incorrect arguments to the grab executor: %s' % repr(args))

    def _execute(self):

        urls = self.args['urls']
        version = self.args['version']

        ciel.log.error('Starting to fetch URLs', 'FETCHEXECUTOR', logging.INFO)
        
        for i, url in enumerate(urls):
            ref = self.block_store.get_ref_for_url(url, version, self.task_id)
            self.task_record.publish_ref(ref)
            out_str = simplejson.dumps(ref, cls=SWReferenceJSONEncoder)
            self.block_store.cache_object(ref, "json", self.output_ids[i])
            self.output_refs[i] = SWDataValue(self.output_ids[i], out_str)

        ciel.log.error('Done fetching URLs', 'FETCHEXECUTOR', logging.INFO)
            
class SyncExecutor(SimpleExecutor):

    handler_name = "sync"
    
    def __init__(self, block_store):
        SimpleExecutor.__init__(self, block_store)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        SimpleExecutor.check_args_valid(args, n_outputs)
        if "inputs" not in args or n_outputs != 1:
            raise BlameUserException('Incorrect arguments to the sync executor: %s' % repr(self.args))            

    def _execute(self):
        reflist = [self.task_record.retrieve_ref(x) for x in self.args["inputs"]]
        self.output_refs[0] = self.block_store.ref_from_object(reflist, "json", self.output_ids[0])

def build_init_descriptor(handler, args, package_ref):
    return {"handler": "init", 
            "dependencies": [package_ref], 
            "task_private": {"package_ref": package_ref, 
                             "start_handler": handler, 
                             "start_args": args
                             } 
            }

class InitExecutor:

    handler_name = "init"

    def __init__(self, block_store):
        pass

    @staticmethod
    def can_run():
        return True

    @classmethod
    def build_task_descriptor(cls, descriptor, parent_task_record, **args):
        raise BlameUserException("Can't spawn init tasks directly; build them from outside the cluster using 'build_init_descriptor'")

    def run(self, task_descriptor, task_record):
        
        args_dict = task_descriptor["task_private"]["start_args"]
        # Some versions of simplejson make these ascii keys into unicode objects :(
        args_dict = dict([(str(k), v) for (k, v) in args_dict.items()])
        initial_task_out_refs = spawn_other(task_record,
                                            task_descriptor["task_private"]["start_handler"], 
                                            True,
                                            **args_dict)

        # Spawn this one manually so I can delegate my output
        final_task_descriptor = {"handler": "sync",
                                 "expected_outputs": task_descriptor["expected_outputs"],
                                 "task_private": {"hint": "small_task"}}
        task_record.spawn_task(final_task_descriptor, args={"inputs": initial_task_out_refs}, n_outputs=1)

    def cleanup(self):
        pass
