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
    SWDataValue, SW2_StreamReference, SWErrorReference, SW2_SweetheartReference, SW2_TombstoneReference,\
    SW2_FixedReference
from skywriting.runtime.references import SWReferenceJSONEncoder
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    BlameUserException, MissingInputException
from shared.skypy_spawn import SkyPySpawn

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
from skywriting.runtime.block_store import STREAM_RETRY, json_decode_object_hook
from errno import EPIPE

import ciel
import struct

try:
    from shared.generated.ciel.protoc_pb2 import Task
    from shared.generated.java2.protoc_pb2 import Java2TaskPrivate
except:
    pass

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

class list_with:
    def __init__(self, l):
        self.wrapped_list = l
    def __enter__(self):
        return [x.__enter__() for x in self.wrapped_list]
    def __exit__(self, exnt, exnv, exntb):
        for x in self.wrapped_list:
            x.__exit__(exnt, exnv, exntb)
        return False

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = dict([(x.handler_name, x) for x in [SkywritingExecutor, SkyPyExecutor, SWStdinoutExecutor, 
                                                           EnvironmentExecutor, JavaExecutor, DotNetExecutor, 
                                                           CExecutor, GrabURLExecutor, SyncExecutor, InitExecutor,
                                                           Java2Executor, ProcExecutor]])
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

    def get_executor(self, name, worker):
        try:
            return self.runnable_executors[name](worker)
        except KeyError:
            raise Exception("Can't run %s here" % name)

    def get_executor_class(self, name):
        return self.executors[name]

# Helper functions
def spawn_task_helper(task_record, executor_name, small_task=False, **executor_args):

    new_task_descriptor = {"handler": executor_name}
    if small_task:
        try:
            worker_private = new_task_descriptor["worker_private"]
        except KeyError:
            worker_private = {}
            new_task_descriptor["worker_private"] = worker_private
        worker_private["hint"] = "small_task"
    return task_record.spawn_task(new_task_descriptor, **executor_args)

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

class BaseExecutor:
    
    TASK_PRIVATE_ENCODING = 'pickle'
    
    def __init__(self, worker):
        self.worker = worker
        self.block_store = worker.block_store
    
    def run(self, task_descriptor, task_record):
        # XXX: This is braindead, considering that we just stashed task_private
        #      in here during prepare().
        self._run(task_descriptor["task_private"], task_descriptor, task_record)
    
    def abort(self):
        self._abort()    
    
    def cleanup(self):
        pass
        
    @classmethod
    def prepare_task_descriptor_for_execute(cls, task_descriptor, block_store):
        # Convert task_private from a reference to an object in here.
        try:
            task_descriptor["task_private"] = block_store.retrieve_object_for_ref(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING)
        except:
            ciel.log('Error retrieving task_private reference from task', 'BASE_EXECUTOR', logging.WARN, True)
            raise
        
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store):
        # Convert task_private to a reference in here. 
        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        task_private_ref = block_store.ref_from_object(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING, task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)

class AsyncFetchCallbackCatcher:

    def __init__(self, ref, chunk_size):
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.bytes = 0
        self.ref = ref
        self.chunk_size = chunk_size
        self.done = False

    def set_fetch_client(self, client):
        self.client = client

    def progress(self, bytes):
        with self.lock:
            self.bytes = bytes
            self.condvar.notify_all()

    def result(self, success):
        with self.lock:
            self.done = True
            self.success = success
            self.condvar.notify_all()

    def reset(self):
        with self.lock:
            self.done = True
            self.success = False
            self.condvar.notify_all()
        self.client.cancel()

    def wait_bytes(self, bytes):
        with self.lock:
            while self.bytes < bytes and not self.done:
                self.condvar.wait()

    def wait_eof(self):
        with self.lock:
            while not self.done:
                self.condvar.wait()

    def cancel(self):
        self.client.cancel()

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if not self.done:
            ciel.log("Cancelling async fetch for %s" % self.ref, "EXEC", logging.WARNING)
            self.cancel()
        return False

class ContextManager:
    def __init__(self, description):
        self.description = description
        self.active_contexts = []

    def add_context(self, new_context):
        ret = new_context.__enter__()
        self.active_contexts.append(ret)
        return ret
    
    def remove_context(self, context):
        self.active_contexts.remove(context)
        context.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            ciel.log("Context manager for %s exiting with exception %s" % (self.description, repr(exnv)), "EXEC", logging.WARNING)
        else:
            ciel.log("Context manager for %s exiting cleanly" % self.description, "EXEC", logging.INFO)
        for ctx in self.active_contexts:
            ctx.__exit__(exnt, exnv, exnbt)
        return False

class SkyPyOutput:

    def __init__(self, output_ctx, executor):
        self.output_ctx = output_ctx
        self.watch_chunk_size = None
        self.executor = executor

    def __enter__(self):
        return self

    def size_update(self, new_size):
        self.output_ctx.size_update(new_size)

    def close(self):
        self.output_ctx.close()

    def rollback(self):
        self.output_ctx.rollback()

    def set_chunk_size(self, new_chunk_size):
        if self.watch_chunk_size is not None:
            self.executor._subscribe_output(self.output_ctx.refid, new_chunk_size)
        self.watch_chunk_size = new_chunk_size

    def get_stream_ref(self):
        return self.output_ctx.get_stream_ref()

    def get_completed_ref(self):
        return self.output_ctx.get_completed_ref()

    def start(self):
        self.executor._subscribe_output(self.output_ctx.refid, self.watch_chunk_size)

    def cancel(self):
        self.watch_chunk_size = None
        self.executor._unsubscribe_output(self.output_ctx.refid)

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            self.rollback()
        else:
            self.close()

class SkyPyExecutor(BaseExecutor):

    handler_name = "skypy"
    
    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)
        self.skypybase = os.getenv("CIEL_SKYPY_BASE")
        self.proc = None
        self.transmit_lock = threading.Lock()

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, pyfile_ref=None, coro_data=None, entry_point=None, entry_args=None, export_json=False, n_extra_outputs=0, cont_delegated_outputs=None, extra_dependencies=[]):

        if pyfile_ref is None:
            raise BlameUserException("All SkyPy invocations must specify a .py file reference as 'pyfile_ref'")
        if coro_data is not None:
            coro_ref = coro_data.toref("%s:coro" % task_descriptor["task_id"])
            parent_task_record.publish_ref(coro_ref)
            task_descriptor["task_private"]["coro_ref"] = coro_ref
            task_descriptor["dependencies"].append(coro_ref)
        else:
            task_descriptor["task_private"]["entry_point"] = entry_point
            task_descriptor["task_private"]["entry_args"] = entry_args
        if cont_delegated_outputs is None:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"].append(ret_output)
            task_descriptor["task_private"]["ret_output"] = ret_output
            extra_outputs = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(n_extra_outputs)]
            task_descriptor["expected_outputs"].extend(extra_outputs)
            task_descriptor["task_private"]["extra_outputs"] = extra_outputs
            task_descriptor["task_private"]["export_json"] = export_json
            task_descriptor["task_private"]["is_continuation"] = False
        else:
            task_descriptor["expected_outputs"].extend(cont_delegated_outputs)
            task_descriptor["task_private"]["is_continuation"] = True
        task_descriptor["dependencies"].extend(extra_dependencies)
        task_descriptor["task_private"]["py_ref"] = pyfile_ref
        task_descriptor["dependencies"].append(pyfile_ref)
        add_package_dep(parent_task_record.package_ref, task_descriptor)

        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record, block_store)

        if cont_delegated_outputs is not None:
            return None
        elif n_extra_outputs == 0:
            return SW2_FutureReference(ret_output)
        else:
            return SkyPySpawn(SW2_FutureReference(ret_output), [SW2_FutureReference(x) for x in extra_outputs])

    @staticmethod
    def can_run():
        if "CIEL_SKYPY_BASE" not in os.environ:
            ciel.log.error("Can't run SkyPy: CIEL_SKYPY_BASE not in environment", "SKYPY", logging.WARNING)
            return False
        else:
            return test_program(["pypy", os.getenv("CIEL_SKYPY_BASE") + "/stub.py", "--version"], "PyPy")

    def _run(self, task_private, task_descriptor, task_record):
        
        with ContextManager("SkyPy task %s" % task_descriptor["task_id"]) as manager:
            self.context_manager = manager
            self._guarded_run(task_private, task_descriptor, task_record)

    def _guarded_run(self, task_private, task_descriptor, task_record):

        self.task_descriptor = task_descriptor
        self.task_record = task_record
        halt_dependencies = []
        skypy_private = task_private

        self.ongoing_fetches = []
        self.ongoing_outputs = dict()

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

        pypy_args = ["pypy", self.skypybase + "/stub.py"]
            
        self.pypy_process = subprocess.Popen(pypy_args, env=pypy_env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        # Handle used for aborting the process.
        self.proc = pypy_process

        if "coro_ref" not in skypy_private:
            start_dict = {"entry_point": skypy_private["entry_point"], "entry_args": skypy_private["entry_args"]}
        else:
            start_dict = {"coro_filename": coroutine_filename}
        start_dict.update({"source_filename": py_source_filename,
                           "is_continuation": skypy_private["is_continuation"]})
        if not skypy_private["is_continuation"]:
            start_dict.update({"extra_outputs": skypy_private["extra_outputs"], 
                               "ret_output": skypy_private["ret_output"], 
                               "export_json": skypy_private["export_json"]})
        pickle.dump(start_dict, self.pypy_process.stdin)

        while True:
            
            request_args = pickle.load(self.pypy_process.stdout)
            request = request_args["request"]
            del request_args["request"]
            ciel.log.error("Request: %s" % request, "SKYPY", logging.DEBUG)
            ret = None
            # The key difference between deref and deref_json is that the JSON variant MUST be decoded locally
            # This is a hack around the fact that the simplejson library hasn't been ported to pypy.
            if request == "deref":
                try:
                    ret = self.deref_func(**request_args)
                except ReferenceUnavailableException:
                    halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
            elif request == "deref_json":
                try:
                    ret = self.deref_json(**request_args)
                except ReferenceUnavailableException:
                    halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
            elif request == "deref_async":
                try:
                    ret = self.deref_async(**request_args)
                except ReferenceUnavailableException:
                    halt_dependencies.append(request_args["ref"])
                    ret = {"success": False}
            elif request == "wait_stream":
                ret = self.wait_async_file(**request_args)
            elif request == "close_stream":
                self.close_async_file(request_args["id"], request_args["chunk_size"])
            elif request == "spawn":
                coro_descriptor = request_args["coro_descriptor"]
                del request_args["coro_descriptor"]
                out_obj = self.spawn_func(coro_descriptor, **request_args)
                ret = {"outputs": out_obj}
            elif request == "exec":
                out_refs = spawn_task_helper(self.task_record, **request_args)
                ret = {"outputs": out_refs}
            elif request == "create_fresh_output":
                new_output_name = self.task_record.create_published_output_name()
                ret = {"name": new_output_name}
            elif request == "open_output":
                filename = self.open_output(**request_args)
                ret = {"filename": filename}
            elif request == "close_output":
                ret_ref = self.close_output(**request_args)
                ret = {"ref": ret_ref}
            elif request == "advert":
                self.output_size_update(**request_args)
            elif request == "rollback_output":
                self.rollback_output(**request_args)
            elif request == "package_lookup":
                ret = {"value": package_lookup(self.task_record, self.block_store, request_args["key"])}
            elif request == "freeze":
                # The interpreter is stopping because it needed a reference that wasn't ready yet.
                if len(self.ongoing_outputs) != 0:
                    raise Exception("SkyPy attempted to freeze with active outputs!")
                coro_data = FileOrString(request_args, self.block_store)
                cont_deps = halt_dependencies
                cont_deps.extend(request_args["additional_deps"])
                spawn_task_helper(self.task_record, 
                                  "skypy", 
                                  small_task=True, 
                                  cont_delegated_outputs=task_descriptor["expected_outputs"], 
                                  extra_dependencies=cont_deps, 
                                  coro_data=coro_data, 
                                  pyfile_ref=pyfile_ref)
                return
            elif request == "done":
                # The interpreter is stopping because the function has completed
                result = FileOrString(request_args, self.block_store)
                if request_args["export_json"]:
                    result_ref = self.block_store.ref_from_object(result.toobj(), "json", request_args["ret_output"])
                else:
                    result_ref = result.toref(request_args["ret_output"])
                self.task_record.publish_ref(result_ref)
                return
            elif request == "exception":
                report_text = FileOrString(request_args, self.block_store).tostr()
                raise Exception("Fatal pypy exception: %s" % report_text)
            else:
                raise Exception("Pypy requested bad operation: %s / %s" % (request, request_args))
            
            if ret is not None:
                ret["request"] = request
                with self.transmit_lock:
                    pickle.dump(ret, self.pypy_process.stdin)

    # Note this is not the same as an external spawn -- it could e.g. spawn an anonymous lambda
    # This is tricky to implement as a spawn_other because we need self.pyfile_ref. Could pass that down to SkyPy at start of day perhaps.
    def spawn_func(self, coro_descriptor, **otherargs):

        coro_data = FileOrString(coro_descriptor, self.block_store)
        return spawn_task_helper(self.task_record, "skypy", coro_data=coro_data, pyfile_ref=self.pyfile_ref, **otherargs)
        
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

    def deref_async(self, ref, chunk_size):
        real_ref = self.task_record.retrieve_ref(ref)
        new_catcher = AsyncFetchCallbackCatcher(real_ref, chunk_size)
        new_fetch = self.block_store.fetch_ref_async(real_ref, 
                                                     result_callback=new_catcher.result,
                                                     progress_callback=new_catcher.progress, 
                                                     reset_callback=new_catcher.reset,
                                                     chunk_size=chunk_size)
        new_catcher.set_fetch_client(new_fetch)
        self.context_manager.add_context(new_catcher)
        self.ongoing_fetches.append(new_catcher)
        ret = {"filename": new_fetch.get_filename(), "done": new_catcher.done, "size": new_catcher.bytes}
        ciel.log("Async fetch %s (chunk %d): initial status %d bytes, done=%s" % (real_ref, chunk_size, ret["size"], ret["done"]), "SKYPY", logging.INFO)
        if new_catcher.done:
            if not new_catcher.success:
                ciel.log("Async fetch %s failed early" % ref, "SKYPY", logging.WARNING)
            ret["success"] = new_catcher.success
        else:
            ret["success"] = True
        return ret

    def close_async_file(self, id, chunk_size):
        for catcher in self.ongoing_fetches:
            if catcher.ref.id == id and catcher.chunk_size == chunk_size:
                self.context_manager.remove_context(catcher)
                self.ongoing_fetches.remove(catcher)
                ciel.log("Cancelling async fetch %s (chunk %d)" % (id, chunk_size), "SKYPY", logging.INFO)
                return
        ciel.log("Ignored cancel for async fetch %s (chunk %d): not in progress" % (id, chunk_size), "SKYPY", logging.WARNING)

    def wait_async_file(self, id, eof=None, bytes=None):
        the_catcher = None
        for catcher in self.ongoing_fetches:
            if catcher.ref.id == id:
                the_catcher = catcher
                break
        if the_catcher is None:
            ciel.log("Failed to wait for async-fetch %s: not an active transfer" % id, "SKYPY", logging.WARNING)
            return {"success": False}
        if eof is not None:
            ciel.log("Waiting for fetch %s to complete" % id, "SKYPY", logging.INFO)
            the_catcher.wait_eof()
        else:
            ciel.log("Waiting for fetch %s length to exceed %d bytes" % (id, bytes), "SKYPY", logging.INFO)
            the_catcher.wait_bytes(bytes)
        if the_catcher.done and not the_catcher.success:
            ciel.log("Wait %s complete: transfer has failed" % id, "SKYPY", logging.WARNING)
            return {"success": False}
        else:
            ret = {"size": the_catcher.bytes, "done": the_catcher.done, "success": True}
            ciel.log("Wait %s complete: new length=%d, EOF=%s" % (id, ret["size"], ret["done"]), "SKYPY", logging.INFO)
            return ret

    def open_output(self, id):
        if id in self.ongoing_outputs:
            raise Exception("SkyPy tried to open output %s which was already open" % id)
        new_output = self.block_store.make_local_output(id, subscribe_callback=self.subscribe_output)
        skypy_output = SkyPyOutput(new_output, self)
        self.ongoing_outputs[id] = skypy_output
        self.context_manager.add_context(skypy_output)
        ref = skypy_output.get_stream_ref()
        self.task_record.prepublish_refs([ref])
        return new_output.get_filename()

    def stop_output(self, id):
        self.context_manager.remove_context(self.ongoing_outputs[id])
        del self.ongoing_outputs[id]

    def close_output(self, id, size):
        output = self.ongoing_outputs[id]
        output.size_update(size)
        self.stop_output(id)
        ret_ref = output.get_completed_ref()
        self.task_record.publish_ref(ret_ref)
        return ret_ref

    def rollback_output(self, id):
        self.ongoing_outputs[id].rollback()
        self.stop_output(id)

    def subscribe_output(self, output_ctx):
        return self.ongoing_outputs[output_ctx.refid]

    def output_size_update(self, id, size):
        self.ongoing_outputs[id].size_update(size)

    def _subscribe_output(self, id, chunk_size):
        with self.transmit_lock:
            pickle.dump({"request": "subscribe", "id": id, "chunk_size": chunk_size}, self.pypy_process.stdin)

    def _unsubscribe_output(self, id):
        with self.transmit_lock:
            pickle.dump({"request": "unsubscribe", "id": id}, self.pypy_process.stdin)

    def _abort(self):
        try:
            if self.proc is not None:
                self.proc.kill()
        except:
            ciel.log('Error killing SkyPy process', 'SKYPY', logging.ERROR, )

# Return states for proc task termination.
PROC_EXITED = 0
PROC_BLOCKED = 1
PROC_ERROR = 2

class ProcExecutor(BaseExecutor):
    """Executor for running long-lived legacy processes."""
    
    handler_name = "proc"
    
    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)
        self.process_pool = worker.process_pool

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, process_record_id, blocking_refs, current_outputs, output_mask):

        task_descriptor["task_private"]["id"] = process_record_id
        task_descriptor["task_private"]["blocking_refs"] = blocking_refs
        task_descriptor["dependencies"].extend(blocking_refs)

        task_descriptor["expected_outputs"] = []

        for i, id in enumerate(current_outputs):
            if i not in output_mask:
                task_descriptor["expected_outputs"].append(id)

        task_private_id = ("%s:_private" % task_descriptor["task_id"])        
        task_private_ref = SW2_FixedReference(task_private_id, block_store.netloc)
        block_store.write_fixed_ref_string(pickle.dumps(task_descriptor["task_private"]), task_private_ref)
        parent_task_record.publish_ref(task_private_ref)
        
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)

    @staticmethod
    def can_run():
        ciel.log('proc executor enabled', 'PROC', logging.INFO)
        return True
    
    def _run(self, task_private, task_descriptor, task_record):
        
        print task_private
        
        id = task_private['id']
        self.process_record = self.process_pool.get_process_record(id)
        
        self.task_record = task_record
        self.task_descriptor = task_descriptor
        
        self.expected_outputs = self.task_descriptor['expected_outputs']
        
        # Stores the indices of expected outputs that have been produced.
        self.open_output_contexts = {}
        self.expected_output_mask = set()
        
        # XXX: This will block until the attached process opens the pipes.
        reader = self.process_record.get_read_fifo()
        writer = self.process_record.get_write_fifo()
        
        ciel.log('Got reader and writer FIFOs', 'PROC', logging.INFO)
        
        try:
            # If we are resuming, we need to unblock the process by replying to the block RPC.
            prev_block_refs = task_private['blocking_refs']
            assert self.process_record.protocol == 'json'
            reply_refs = [self.task_record.retrieve_ref(x) for x in prev_block_refs]

            response_string = simplejson.dumps(reply_refs, cls=SWReferenceJSONEncoder)
            ciel.log('Writing %d bytes response to blocking RPC' % len(response_string), 'PROC', logging.INFO)
            writer.write(struct.pack('!I', len(response_string)))
            writer.write(response_string)
            writer.flush()
            
        except:
            pass
        
        
        try:
            if self.process_record.protocol == 'line':
                finished = self.line_event_loop(reader, writer)
            elif self.process_record.protocol == 'json':
                finished = self.json_event_loop(reader, writer)
            elif self.process_record.protocol == 'pickle':
                finished = self.pickle_event_loop(reader, writer)
            elif self.process_record.protocol == 'protobuf':
                finished = self.protobuf_event_loop(reader, writer)
            else:
                raise BlameUserException('Unsupported protocol: %s' % self.process_record.protocol)
        except:
            ciel.log('Got unexpected error', 'PROC', logging.ERROR, True)
            finished = PROC_ERROR
        
        if finished == PROC_EXITED:
            
            if len(self.expected_output_mask) != len(self.expected_outputs):
                # Not all outputs have been produced, but we have exited.
                # So let's publish error references for those.
                for i, id in enumerate(self.expected_outputs):
                    if i not in self.expected_output_mask:
                        ciel.log('No output written for output[%d] = %s' % (i, id), 'PROC', logging.ERROR)
                        task_record.publish_ref(SWErrorReference(id, 'OUTPUT_NOT_WRITTEN', str(i))) 
                            
            self.process_pool.delete_process_record(self.process_record)
            
        elif finished == PROC_BLOCKED:
            pass
        elif finished == PROC_ERROR:
            ciel.log('Task died with an error', 'PROC', logging.ERROR)
            for output_id in self.expected_outputs:
                task_record.publish_ref(SWErrorReference(output_id, 'RUNTIME_ERROR', None))
            self.process_pool.delete_process_record(self.process_record)
        
    def line_event_loop(self, reader, writer):
        """Dummy event loop for testing interactive tasks."""
        while True:
            line = reader.readline()
            if line == '':
                return True
            
            argv = line.split()
            
            if argv[0] == 'exit':
                return True
            elif argv[0] == 'echo':
                print argv[1:]
            elif argv[0] == 'filename':
                print argv[1]
            else:
                print 'Unrecognised command:', argv
        
    def open_ref(self, ref):
        """Fetches a reference if it is available, and returns a filename for reading it.
        Options to do with eagerness, streaming, etc.
        If reference is unavailable, raises a ReferenceUnavailableException."""
        ref = self.task_record.retrieve_ref(ref)
        return self.block_store.retrieve_filename_for_ref(ref)
        
    def spawn(self, request_args):
        """Spawns a child task. Arguments define a task_private structure. Returns a list
        of future references."""
        
        # Args dict arrives from sw with unicode keys :(
        str_args = dict([(str(k), v) for (k, v) in request_args.items()])
        
        try:
            small_task = str_args['small_task']
        except KeyError:
            str_args['small_task'] = False
        
        return spawn_task_helper(self.task_record, **str_args)
    
    def create_ref_inline(self, content):
        """Creates a new reference, with the given string contents."""
        ref = self.block_store.ref_from_string(content, self.task_record.create_published_output_name())
        self.task_record.publish_ref(ref)
        return ref
    
    def create_ref_file(self):
        """Creates a new reference, and returns a filename for writing it.
        Also, can supply a string and create the new reference inline.
        Also, can specify what output is being created.
        Returns the created reference to the object, and (if necessary) filename."""
        id = self.task_record.create_published_output_name()
        ctx = self.block_store.make_local_output(id)
        self.open_ref_contexts[ctx.get_filename()] = ctx
        return ctx.get_filename()
    
    def write_output_inline(self, index, content):
        """Creates a concrete object for the output with the given index, having the given string contents."""
        self.expected_output_mask.add(index)
        ref = self.block_store.ref_from_string(content, self.task_descriptor['expected_outputs'][index])
        self.task_record.publish_ref(ref)
        return ref
    
    def write_output_file(self, index):
        """Creates a file for the output with the given index, and returns the filename."""
        ctx = self.block_store.make_local_output(self.expected_outputs[index])
        self.open_output_contexts[index] = ctx
        return ctx.get_filename()

    def close_ref(self, filename):
        """Closes the open file for a constructed reference."""
        ctx = self.open_ref_contexts.pop(filename)
        ctx.close()
        ref = ctx.get_completed_ref()
        self.task_record.publish_ref(ref)
        return ref
    
    def close_output(self, index):
        """Closes the open file for an output."""
        ctx = self.open_output_contexts.pop(index)
        self.expected_output_mask.add(index)
        ctx.close()
        ref = ctx.get_completed_ref()
        self.task_record.publish_ref(ref)
        return ref
    
    def block_on_refs(self, refs):
        """Creates a continuation task for blocking on the given references."""
        
        task_descriptor = {"handler" : "proc"}
        spawn_task_helper(self.task_record, "proc", False, process_record_id=self.process_record.id, blocking_refs=refs, current_outputs=self.expected_outputs, output_mask=self.expected_output_mask)
        
    
    def json_event_loop(self, reader, writer):
        print 'In json_event_loop'
        while True:
            try:
                request_len, = struct.unpack_from('!I', reader.read(4))
                ciel.log('Reading %d bytes request' % request_len, 'PROC', logging.INFO)
                request_string = reader.read(request_len)
            except:
                ciel.log('Error reading in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
        
            try:
                (method, args) = simplejson.loads(request_string, object_hook=json_decode_object_hook)
            except:
                ciel.log('Error parsing JSON request', 'PROC', logging.WARN, True)
                return PROC_ERROR
        
            ciel.log('Method is %s' % repr(method), 'PROC', logging.INFO)
        
            try:
                if method == 'open_ref':
                    
                    try:
                        ref = args['ref']
                    except KeyError:
                        ciel.log('Missing required argument key: ref', 'PROC', logging.ERROR, False)
                        return PROC_ERROR
                    
                    try:
                        response = {'filename' : self.open_ref(ref)}
                    except ReferenceUnavailableException:
                        response = {'error' : 'EWOULDBLOCK'}
                    
                elif method == 'spawn':
                    
                    response = self.spawn(args)
                    
                elif method == 'create_ref':
                    
                    try:
                        # Create ref for inline string.
                        response = {'ref' : self.create_ref_inline(args['inline'])}
                    except KeyError:
                        # Create ref and return filename for writing.
                        response = {'filename' : self.create_ref_file()}
                        
                elif method == 'write_output':
                    
                    try:
                        index = int(args['i'])
                        if index < 0 or index > len(self.task_descriptor['expected_outputs']):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.task_descriptor['expected_outputs'], 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            index = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    try:
                        # Write output with inline string.
                        response = {'ref' : self.write_output_inline(index, args['inline'])}
                    except KeyError:
                        # Return filename for writing.
                        response = {'filename' : self.write_output_file(index)}
                        
                elif method == 'close_ref':
                    
                    try:
                        filename = args['filename']
                    except KeyError:
                        ciel.log('Missing required argument: ref', 'PROC', logging.ERROR, False)
                        return PROC_ERROR
                    
                    response = {'ref' : self.close_ref(filename)}
                        
                elif method == 'close_output':
    
                    try:
                        index = int(args['i'])
                        if index < 0 or index > len(self.task_descriptor['expected_outputs']):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.task_descriptor['expected_outputs'], 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            index = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    response = {'ref' : self.close_output(index)}
    
                elif method == 'block':
                    
                    self.block_on_refs(args)
                    return PROC_BLOCKED
    
                elif method == 'error':
                    ciel.log('Got error from task: %s' % args, 'PROC', logging.ERROR, False)
                    return PROC_ERROR
    
                elif method == 'exit':
                    
                    return PROC_EXITED
                
                else:
                    ciel.log('Invalid method: %s' % method, 'PROC', logging.WARN, False)
                    return PROC_ERROR

            except:
                ciel.log('Error during method handling in JSON event loop', 'PROC', logging.ERROR, True)
                return PROC_ERROR
        
            try:
                response_string = simplejson.dumps(response, cls=SWReferenceJSONEncoder)
                ciel.log('Writing %d bytes response' % len(response_string), 'PROC', logging.INFO)
                writer.write(struct.pack('!I', len(response_string)))
                writer.write(response_string)
                writer.flush()
            except:
                ciel.log('Error writing response in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
        
        return True
    
    def pickle_event_loop(self, reader, writer):
        """Could be on the SkyPy event loop, above"""
        return PROC_ERROR
    
    def protobuf_event_loop(self, reader, writer):
        return PROC_ERROR
        
class Java2Executor(BaseExecutor):
    
    handler_name = "java2"
    
    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store):
        # More good stuff goes here.
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record, block_store)

    @staticmethod
    def can_run():
        return False
        cp = os.getenv("CLASSPATH")
        if cp is None:
            ciel.log.error("Can't run Java: no CLASSPATH set", "JAVA", logging.WARNING)
            return False
        else:
            return test_program(["java", "-cp", cp, "com.asgow.ciel.executor.Java2Executor", "--version"], "Java")

    def _run(self, task_private, task_descriptor, task_record):
        
        # 1. Convert the task descriptor and task-private data to a protobuf.
        
        # 2. Run the JVM, passing in the task protobuf on stdin.
        
        # 3. Read from standard output to get any messages (in protobuf format) from the executor.
        
        # 4. If we get a non-zero exit code, write SWErrorReferences to all of the task's expected outputs.
        
        # 5. Communicate the report back to the master.
        
        pass

    def spawn_func(self, **otherargs):
        private_data = FileOrString(otherargs, self.block_store)
        new_task_descriptor = {"handler": "skypy",
                               "task_private": private_data}
        
        self.task_record.spawn_task(new_task_descriptor)

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
            filenames = self.block_store.retrieve_filenames_for_refs_eager([real_ref])
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

class SkywritingExecutor(BaseExecutor):

    handler_name = "swi"

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)
        self.stdlibbase = os.getenv("CIEL_SW_STDLIB", None)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, sw_file_ref=None, start_env=None, start_args=None, cont=None, cont_delegated_output=None, extra_dependencies={}):

        if cont_delegated_output is None:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"] = [ret_output]
        else:
            task_descriptor["expected_outputs"] = [cont_delegated_output]
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
        task_descriptor["dependencies"].extend(extra_dependencies)
        add_package_dep(parent_task_record.package_ref, task_descriptor)
        
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record, block_store)

        if cont_delegated_output is not None:
            return None
        else:
            return SW2_FutureReference(ret_output)

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

    def _run(self, task_private, task_descriptor, task_record):

        sw_private = task_private
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
           
            spawn_task_helper(self.task_record, 
                              "swi", 
                              small_task=True, 
                              cont_delegated_output=task_descriptor["expected_outputs"][0], 
                              extra_dependencies=list(self.lazy_derefs), 
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
        return spawn_task_helper(self.task_record, "swi", True, cont=cont)

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
        ret = spawn_task_helper(self.task_record, executor_name, small_task, **str_args)
        if isinstance(ret, SWRealReference):
            return ret
        else:
            return list(ret)

    def spawn_exec_func(self, executor_name, args, num_outputs):
        return self.spawn_other(executor_name, {"args": args, "n_outputs": num_outputs})

    def exec_func(self, executor_name, args, num_outputs):
        return self.spawn_other(executor_name, {"args": args, "n_outputs": num_outputs, "small_task": True})

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

class SimpleExecutor(BaseExecutor):

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, block_store, args, n_outputs, delegated_outputs=None):

        if delegated_outputs is not None and len(delegated_outputs) != n_outputs:
            raise BlameUserException("SimpleExecutor being built with delegated outputs %s but n_outputs=%d" % (delegated_outputs, n_outputs))

        # Throw early if the args are bad
        cls.check_args_valid(args, n_outputs)

        # Discover required ref IDs for this executor
        reqd_refs = cls.get_required_refs(args)
        task_descriptor["dependencies"].extend(reqd_refs)

        sha = hashlib.sha1()
        hash_update_with_structure(sha, [args, n_outputs])
        name_prefix = "%s:%s:" % (cls.handler_name, sha.hexdigest())

        # Name our outputs
        if delegated_outputs is None:
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]
        else:
            task_descriptor["expected_outputs"] = delegated_outputs

        # Add the args dict
        args_name = "%ssimple_exec_args" % name_prefix
        args_ref = block_store.ref_from_object(args, "pickle", args_name)
        parent_task_record.publish_ref(args_ref)
        task_descriptor["dependencies"].append(args_ref)
        task_descriptor["task_private"]["simple_exec_args"] = args_ref
        
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record, block_store)

        if delegated_outputs is not None:
            return None
        else:
            return [SW2_FutureReference(x) for x in task_descriptor["expected_outputs"]]
        
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

    def _run(self, task_private, task_descriptor, task_record):
        self.task_record = task_record
        self.task_id = task_descriptor["task_id"]
        self.output_ids = task_descriptor["expected_outputs"]
        self.output_refs = [None for i in range(len(self.output_ids))]
        self.succeeded = False
        self.args = self.block_store.retrieve_object_for_ref(task_private["simple_exec_args"], "pickle")

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

class AsyncPushThread:

    def __init__(self, block_store, ref, chunk_size=67108864):
        self.block_store = block_store
        self.ref = ref
        self.chunk_size = chunk_size
        self.next_threshold = self.chunk_size
        self.success = None
        self.lock = threading.Lock()
        self.fetch_done = False
        self.stream_done = False
        self.stream_started = False
        self.bytes_copied = 0
        self.bytes_available = 0
        self.condvar = threading.Condition(self.lock)
        self.thread = None
        self.filename = None

    def start(self, fifos_dir):
        self.fifos_dir = fifos_dir
        self.file_fetch = self.block_store.fetch_ref_async(self.ref,
                                                           chunk_size=self.chunk_size,
                                                           result_callback=self.result, 
                                                           reset_callback=self.reset, 
                                                           progress_callback=self.progress)
        if not self.fetch_done:
            self.thread = threading.Thread(target=self.thread_main)
            self.thread.start()
        else:
            ciel.log("Fetch for %s completed before first read; using file directly" % self.ref, "EXEC", logging.INFO)
            with self.lock:
                self.filename = self.file_fetch.get_filename()
                self.stream_done = True
                self.condvar.notify_all()

    def thread_main(self):

        with self.lock:
            while self.bytes_available < self.next_threshold and not self.fetch_done:
                self.condvar.wait()
            if self.fetch_done:
                ciel.log("Fetch for %s completed before we got %d bytes: using file directly" % (self.ref, self.chunk_size), "EXEC", logging.INFO)
                self.stream_done = True
                self.filename = self.file_fetch.get_filename()
                self.condvar.notify_all()
                return
            else:
                self.stream_started = True
        ciel.log("Fetch for %s got more than %d bytes; commencing asynchronous push" % (self.ref, self.chunk_size), "EXEC", logging.INFO)
        self.copy_loop()

    def copy_loop(self):
        
        try:
            read_filename = self.file_fetch.get_filename()
            fifo_name = os.path.join(self.fifos_dir, "fifo-%s" % self.ref.id)
            os.mkfifo(fifo_name)
            with self.lock:
                self.filename = fifo_name
                self.condvar.notify_all()
            with open(read_filename, "r") as input_fp:
                with open(fifo_name, "w") as output_fp:
                    while True:
                        while True:
                            buf = input_fp.read(4096)
                            output_fp.write(buf)
                            self.bytes_copied += len(buf)
                            with self.lock:
                                if self.success is False or (self.bytes_copied == self.bytes_available and self.fetch_done):
                                    self.stream_done = True
                                    self.condvar.notify_all()
                                    ciel.log("FIFO-push for %s complete (success: %s)" % (self.ref, self.success), "EXEC", logging.INFO)
                                    return
                            if len(buf) < 4096:
                                # EOF, for now.
                                break
                        with self.lock:
                            self.next_threshold = self.bytes_copied + self.chunk_size
                            while self.bytes_available < self.next_threshold and not self.fetch_done:
                                self.condvar.wait()
        except Exception as e:
            ciel.log("Push thread for %s died with exception %s" % (read_filename, e), "EXEC", logging.WARNING)
            with self.lock:
                self.stream_done = True
                self.condvar.notify_all()
                
    def get_completed_ref(self, is_sweetheart):
        return self.file_fetch.get_completed_ref(is_sweetheart)

    def result(self, success):
        with self.lock:
            if self.success is None:
                self.success = success
                self.fetch_done = True
                self.condvar.notify_all()
            # Else we've already failed due to a reset.

    def progress(self, bytes_downloaded):
        with self.lock:
            self.bytes_available = bytes_downloaded
            if self.bytes_available >= self.next_threshold:
                self.condvar.notify_all()

    def reset(self):
        ciel.log("Reset of streamed fetch for %s!" % self.ref, "EXEC", logging.WARNING)
        should_cancel = False
        with self.lock:
            if self.stream_started:
                should_cancel = True
        if should_cancel:
            ciel.log("FIFO-stream had begun: failing transfer", "EXEC", logging.ERROR)
            self.file_fetch.cancel()

    def get_filename(self):
        with self.lock:
            while self.filename is None and self.success is not False:
                self.condvar.wait()
            if self.filename is not None:
                return self.filename
            else:
                raise Exception("Transfer for fetch thread %s failed" % self.ref)

    def cancel(self):
        self.file_fetch.cancel()

    def wait(self):
        with self.lock:
            while not self.stream_done:
                self.condvar.wait()

class AsyncPushGroup:

    def __init__(self, threads, sweethearts, task_record):
        self.threads = threads
        self.sweethearts = sweethearts
        self.task_record = task_record

    def __enter__(self):
        self.fifos_dir = tempfile.mkdtemp(prefix="ciel-fetch-fifos-")
        for thread in self.threads:
            thread.start(self.fifos_dir)
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            ciel.log("AsyncPushGroup exiting with exception %s: cancelling threads..." % repr(exnv), "EXEC", logging.WARNING)
            for thread in self.threads:
                thread.cancel()
        ciel.log("Waiting for push threads to complete", "EXEC", logging.INFO)
        for thread in self.threads:
            thread.wait()
        shutil.rmtree(self.fifos_dir)
        failed_threads = filter(lambda t: not t.success, self.threads)
        failure_bindings = dict([(ft.ref.id, SW2_TombstoneReference(ft.ref.id, ft.ref.location_hints)) for ft in failed_threads])
        if len(failure_bindings) > 0:
            for fail in failure_bindings:
                ciel.log("Failed fetch: %s" % fail, "EXEC", logging.WARNING)
            if exnt is not None:
                ciel.log("Transfers have failed: replacing old exception with MissingInputException", "EXEC", logging.WARNING)
            raise MissingInputException(failure_bindings)
        else:
            extra_publishes = [t.get_completed_ref(t.ref.id in self.sweethearts) for t in self.threads]
            extra_publishes = filter(lambda x: x is not None, extra_publishes)
            if len(extra_publishes) > 0:
                self.task_record.prepublish_refs(extra_publishes)

        return False

class DummyPushThread:

    def __init__(self, filename):
        self.filename = filename
        
    def get_filename(self):
        return self.filename

    def wait(self):
        pass

class DummyPushGroup:

    def __init__(self, dummys):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        return False

class ProcessRunningExecutor(SimpleExecutor):

    def __init__(self, worker):
        SimpleExecutor.__init__(self, worker)

        self._lock = threading.Lock()
        self.proc = None

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
            push_threads = [DummyPushThread(x) for x in self.block_store.retrieve_filenames_for_refs(self.input_refs)]
            push_ctx = DummyPushGroup(push_threads)
        else:
            push_threads = [AsyncPushThread(self.block_store, ref) for ref in self.input_refs]
            push_ctx = AsyncPushGroup(push_threads, self.make_sweetheart, self.task_record)

        with push_ctx:

            file_inputs = [push_thread.get_filename() for push_thread in push_threads]
        
            with list_with([self.block_store.make_local_output(id) for id in self.output_ids]) as out_file_contexts:

                file_outputs = [ctx.get_filename() for ctx in out_file_contexts]
        
                if self.stream_output:
           
                    stream_refs = [ctx.get_stream_ref() for ctx in out_file_contexts]
                    self.task_record.prepublish_refs(stream_refs)

                self.proc = self.start_process(file_inputs, file_outputs)
                add_running_child(self.proc)

                rc = self.await_process(file_inputs, file_outputs)
                remove_running_child(self.proc)

                self.proc = None

                #        if "trace_io" in self.debug_opts:
                #            transfer_ctx.log_traces()

                if rc != 0:
                    raise OSError()

        for i, output in enumerate(out_file_contexts):
            self.output_refs[i] = output.get_completed_ref()

        ciel.engine.publish("worker_event", "Executor: Done")

    def start_process(self, input_files, output_files):
        raise Exception("Must override start_process when subclassing ProcessRunningExecutor")
        
    def await_process(self, input_files, output_files):
        rc = self.proc.wait()
        return rc

    def _cleanup_task(self):
        pass

    def _abort(self):
        if self.proc is not None:
            self.proc.kill()

class SWStdinoutExecutor(ProcessRunningExecutor):
    
    handler_name = "stdinout"

    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

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

    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        ProcessRunningExecutor.check_args_valid(args, n_outputs)
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))

    def start_process(self, input_files, output_files):

        command_line = self.args["command_line"]

        try:
            env = self.args['env']
        except KeyError:
            env = {}

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
        
        environment.update(env)
            
        proc = subprocess.Popen(map(str, command_line), env=environment, close_fds=True)

        #_ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        return proc

class FilenamesOnStdinExecutor(ProcessRunningExecutor):
    
    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

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

    def __init__(self, worker):
        FilenamesOnStdinExecutor.__init__(self, worker)

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

    def __init__(self, worker):
        FilenamesOnStdinExecutor.__init__(self, worker)

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

    def __init__(self, worker):
        FilenamesOnStdinExecutor.__init__(self, worker)

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
    
    def __init__(self, worker):
        SimpleExecutor.__init__(self, worker)
    
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
    
    def __init__(self, worker):
        SimpleExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        SimpleExecutor.check_args_valid(args, n_outputs)
        if "inputs" not in args or n_outputs != 1:
            raise BlameUserException('Incorrect arguments to the sync executor: %s' % repr(args))            

    def _execute(self):
        reflist = [self.task_record.retrieve_ref(x) for x in self.args["inputs"]]
        self.output_refs[0] = self.block_store.ref_from_object(reflist, "json", self.output_ids[0])

# XXX: Passing ref_of_string to get round a circular import. Should really move ref_of_string() to
#      a nice utility package.
def build_init_descriptor(handler, args, package_ref, master_uri, ref_of_string):
    task_private_dict = {"package_ref": package_ref, 
                         "start_handler": handler, 
                         "start_args": args
                         } 
    task_private_ref = ref_of_string(pickle.dumps(task_private_dict), master_uri)
    return {"handler": "init", 
            "dependencies": [package_ref, task_private_ref], 
            "task_private": task_private_ref
            }

class InitExecutor(BaseExecutor):

    handler_name = "init"

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @staticmethod
    def can_run():
        return True

    @classmethod
    def build_task_descriptor(cls, descriptor, parent_task_record, **args):
        raise BlameUserException("Can't spawn init tasks directly; build them from outside the cluster using 'build_init_descriptor'")

    def _run(self, task_private, task_descriptor, task_record):
        
        args_dict = task_private["start_args"]
        # Some versions of simplejson make these ascii keys into unicode objects :(
        args_dict = dict([(str(k), v) for (k, v) in args_dict.items()])
        initial_task_out_obj = spawn_task_helper(task_record,
                                                 task_private["start_handler"], 
                                                 True,
                                                 **args_dict)
        if isinstance(initial_task_out_obj, SWRealReference):
            initial_task_out_refs = [initial_task_out_obj]
        else:
            initial_task_out_refs = list(initial_task_out_obj)
        spawn_task_helper(task_record, "sync", True, delegated_outputs = task_descriptor["expected_outputs"], args = {"inputs": initial_task_out_refs}, n_outputs=1)
