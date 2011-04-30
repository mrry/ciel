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
    SWRealReference, SW2_FutureReference, SWDataValue, \
    SWErrorReference, SW2_SweetheartReference, SW2_TombstoneReference,\
    SW2_FixedReference, SWReferenceJSONEncoder, SW2_ConcreteReference
from shared.io_helpers import read_framed_json, write_framed_json
from skywriting.runtime.exceptions import BlameUserException, MissingInputException, ReferenceUnavailableException,\
    RuntimeSkywritingError
from skywriting.runtime.executor_helpers import ContextManager, retrieve_filename_for_ref, \
    retrieve_filenames_for_refs, get_ref_for_url, ref_from_string, \
    retrieve_file_or_string_for_ref, ref_from_safe_string,\
    write_fixed_ref_string
from skywriting.runtime.block_store import get_own_netloc

from skywriting.runtime.producer import make_local_output
from skywriting.runtime.fetcher import fetch_ref_async
from skywriting.runtime.object_cache import retrieve_object_for_ref, ref_from_object,\
    cache_object

import hashlib
import simplejson
import logging
import shutil
import subprocess
import tempfile
import os.path
import threading
import pickle
import time
from subprocess import PIPE
from datetime import datetime

from errno import EPIPE

import ciel
import traceback

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
                                                           Java2Executor, OCamlExecutor, ProcExecutor]])
        self.runnable_executors = dict([(x, self.executors[x]) for x in self.check_executors()])
        cacheable_executors = [SkywritingExecutor, SkyPyExecutor, Java2Executor]
        self.process_cacheing_executors = filter(lambda x: x in self.runnable_executors.values(), cacheable_executors)

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
def spawn_task_helper(task_record, executor_name, small_task=False, delegated_outputs=None, scheduling_class=None, scheduling_type=None, **executor_args):

    new_task_descriptor = {"handler": executor_name}
    if small_task:
        try:
            worker_private = new_task_descriptor["worker_private"]
        except KeyError:
            worker_private = {}
            new_task_descriptor["worker_private"] = worker_private
        worker_private["hint"] = "small_task"
    if scheduling_class is not None:
        new_task_descriptor["scheduling_class"] = scheduling_class
    if scheduling_type is not None:
        new_task_descriptor["scheduling_type"] = scheduling_type
    if delegated_outputs is not None:
        new_task_descriptor["expected_outputs"] = delegated_outputs
    return task_record.spawn_task(new_task_descriptor, is_tail_spawn=(delegated_outputs is not None), **executor_args)

def package_lookup(task_record, block_store, key):
    if task_record.package_ref is None:
        ciel.log.error("Package lookup for %s in task without package" % key, "EXEC", logging.WARNING)
        return None
    package_dict = retrieve_object_for_ref(task_record.package_ref, "pickle")
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
            task_descriptor["task_private"] = retrieve_object_for_ref(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING)
        except:
            ciel.log('Error retrieving task_private reference from task', 'BASE_EXECUTOR', logging.WARN, True)
            raise
        
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record):
        # Convert task_private to a reference in here. 
        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        task_private_ref = ref_from_object(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING, task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)

class OngoingFetch:

    def __init__(self, ref, chunk_size, sole_consumer=False, make_sweetheart=False, must_block=False):
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.bytes = 0
        self.ref = ref
        self.chunk_size = chunk_size
        self.sole_consumer = sole_consumer
        self.make_sweetheart = make_sweetheart
        self.done = False
        self.success = None
        self.filename = None
        self.completed_ref = None
        self.file_blocking = None
        # may_pipe = True because this class is only used for async operations.
        # The only current danger of pipes is that waiting for a transfer to complete might deadlock.
        self.fetch_ctx = fetch_ref_async(ref, 
                                         result_callback=self.result,
                                         progress_callback=self.progress, 
                                         reset_callback=self.reset,
                                         start_filename_callback=self.set_filename,
                                         chunk_size=chunk_size,
                                         may_pipe=True,
                                         must_block=must_block,
                                         sole_consumer=sole_consumer)
        
    def progress(self, bytes):
        with self.lock:
            self.bytes = bytes
            self.condvar.notify_all()

    def result(self, success, completed_ref):
        with self.lock:
            self.done = True
            self.success = success
            self.completed_ref = completed_ref
            self.condvar.notify_all()

    def reset(self):
        with self.lock:
            self.done = True
            self.success = False
            self.condvar.notify_all()
        self.client.cancel()

    def set_filename(self, filename, is_blocking):
        with self.lock:
            self.filename = filename
            self.file_blocking = is_blocking
            self.condvar.notify_all()

    def get_filename(self):
        with self.lock:
            while self.filename is None and self.success is not False:
                self.condvar.wait()
            if self.filename is not None:
                return (self.filename, self.file_blocking)
            else:
                return (None, None)

    def get_completed_ref(self):
        return self.completed_ref

    def wait_bytes(self, bytes):
        with self.lock:
            while self.bytes < bytes and not self.done:
                self.condvar.wait()

    def wait_eof(self):
        with self.lock:
            while not self.done:
                self.condvar.wait()

    def cancel(self):
        self.fetch_ctx.cancel()

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if not self.done:
            ciel.log("Cancelling async fetch for %s" % self.ref, "EXEC", logging.WARNING)
            self.cancel()
        return False

class OngoingOutputWatch:
    
    def __init__(self, ongoing_output):
        self.ongoing_output = ongoing_output
        
    def start(self):
        self.ongoing_output.start_watch()
        
    def set_chunk_size(self, new_size):
        return self.ongoing_output.set_watch_chunk_size(new_size)
    
    def cancel(self):
        return self.ongoing_output.cancel_watch()

class OngoingOutput:

    def __init__(self, output_name, output_index, can_smart_subscribe, may_pipe, make_local_sweetheart, executor):
        kwargs = {"may_pipe": may_pipe}
        if can_smart_subscribe:
            kwargs["subscribe_callback"] = self.subscribe_output
        self.output_ctx = make_local_output(output_name, **kwargs)
        self.may_pipe = may_pipe
        self.make_local_sweetheart = make_local_sweetheart
        self.output_name = output_name
        self.output_index = output_index
        self.watch_chunk_size = None
        self.executor = executor
        self.filename = None

    def __enter__(self):
        return self

    def size_update(self, new_size):
        self.output_ctx.size_update(new_size)

    def close(self):
        self.output_ctx.close()

    def rollback(self):
        self.output_ctx.rollback()

    def get_filename(self):
        if self.filename is None:
            (self.filename, is_fd) = self.output_ctx.get_filename_or_fd()
            assert not is_fd
        return self.filename
    
    def get_size(self):
        assert not self.may_pipe
        assert self.filename is not None
        return os.stat(self.filename).st_size
        
    def get_stream_ref(self):
        return self.output_ctx.get_stream_ref()

    def get_completed_ref(self):
        completed_ref = self.output_ctx.get_completed_ref()
        if isinstance(completed_ref, SW2_ConcreteReference) and self.make_local_sweetheart:
            completed_ref = SW2_SweetheartReference.from_concrete(completed_ref, get_own_netloc())
        return completed_ref

    def subscribe_output(self, _):
        return OngoingOutputWatch(self)

    def start_watch(self):
        self.executor._subscribe_output(self.output_index, self.watch_chunk_size)
        
    def set_watch_chunk_size(self, new_chunk_size):
        if self.watch_chunk_size is not None:
            self.executor._subscribe_output(self.output_index, new_chunk_size)
        self.watch_chunk_size = new_chunk_size

    def cancel_watch(self):
        self.watch_chunk_size = None
        self.executor._unsubscribe_output(self.output_index)

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            self.rollback()
        else:
            self.close()

# Return states for proc task termination.
PROC_EXITED = 0
PROC_MUST_KEEP = 1
PROC_MAY_KEEP = 2
PROC_ERROR = 3

class ProcExecutor(BaseExecutor):
    """Executor for running generic processes."""
    
    handler_name = "proc"
    
    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)
        self.process_pool = worker.process_pool
        self.ongoing_fetches = []
        self.ongoing_outputs = dict()
        self.transmit_lock = threading.Lock()

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, 
                              process_record_id=None, is_fixed=False, command=None, proc_pargs=[], proc_kwargs={}, force_n_outputs=None,
                              n_extra_outputs=0, extra_dependencies=[], is_tail_spawn=False, accept_ref_list_for_single=False):

        #if process_record_id is None and start_command is None:
        #    raise BlameUserException("ProcExecutor tasks must specify either process_record_id or start_command")

        if process_record_id is not None:
            task_descriptor["task_private"]["id"] = process_record_id
        if command is not None:
            task_descriptor["task_private"]["command"] = command
        task_descriptor["task_private"]["proc_pargs"] = proc_pargs
        task_descriptor["task_private"]["proc_kwargs"] = proc_kwargs
        task_descriptor["dependencies"].extend(extra_dependencies)

        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        if is_fixed:
            task_private_ref = SW2_FixedReference(task_private_id, get_own_netloc())
            write_fixed_ref_string(pickle.dumps(task_descriptor["task_private"]), task_private_ref)
        else:
            task_private_ref = ref_from_string(pickle.dumps(task_descriptor["task_private"]), task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)

        if force_n_outputs is not None:        
            if "expected_outputs" in task_descriptor and len(task_descriptor["expected_outputs"]) > 0:
                raise BlameUserException("Task already had outputs, but force_n_outputs is set")
            task_descriptor["expected_outputs"] = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(force_n_outputs)]
        
        if not is_tail_spawn:
            if len(task_descriptor["expected_outputs"]) == 1 and not accept_ref_list_for_single:
                return SW2_FutureReference(task_descriptor["expected_outputs"][0])
            else:
                return [SW2_FutureReference(refid) for refid in task_descriptor["expected_outputs"]]

    def get_command(self):
        raise RuntimeSkywritingError("Attempted to get_command() for an executor that does not define this.")
    
    def get_env(self):
        return {}

    @staticmethod
    def can_run():
        return True
    
    def _run(self, task_private, task_descriptor, task_record):
        
        with ContextManager("Task %s" % task_descriptor["task_id"]) as manager:
            self.context_manager = manager
            self._guarded_run(task_private, task_descriptor, task_record)
            
    def _guarded_run(self, task_private, task_descriptor, task_record):
        
        self.task_record = task_record
        self.task_descriptor = task_descriptor
        self.expected_outputs = self.task_descriptor['expected_outputs']
        
        if "id" in task_private:
            id = task_private['id']
            self.process_record = self.process_pool.get_process_record(id)
        else:
            self.process_record = self.process_pool.get_soft_cache_process(self.__class__, task_descriptor["dependencies"])
            if self.process_record is None:
                self.process_record = self.process_pool.create_process_record(None, "json")
                if "command" in task_private:
                    command = [task_private["command"]]
                else:
                    command = self.get_command()
                command.extend(["--write-fifo", self.process_record.get_read_fifo_name(), 
                                "--read-fifo", self.process_record.get_write_fifo_name()])
                new_proc_env = os.environ.copy()
                new_proc_env.update(self.get_env())
                new_proc = subprocess.Popen(command, env=new_proc_env, close_fds=True)
                self.process_record.set_pid(new_proc.pid)
               
        # XXX: This will block until the attached process opens the pipes.
        reader = self.process_record.get_read_fifo()
        writer = self.process_record.get_write_fifo()
        self.reader = reader
        self.writer = writer
        
        #ciel.log('Got reader and writer FIFOs', 'PROC', logging.INFO)

        write_framed_json(("start_task", task_private), writer)

        try:
            if self.process_record.protocol == 'line':
                finished = self.line_event_loop(reader, writer)
            elif self.process_record.protocol == 'json':
                finished = self.json_event_loop(reader, writer)
            else:
                raise BlameUserException('Unsupported protocol: %s' % self.process_record.protocol)
        except:
            ciel.log('Got unexpected error', 'PROC', logging.ERROR, True)
            finished = PROC_ERROR
        
        if finished == PROC_EXITED:
            
            self.process_pool.delete_process_record(self.process_record)
        
        elif finished == PROC_MAY_KEEP:
            self.process_pool.soft_cache_process(self.process_record, self.__class__, self.soft_cache_keys)    
        
        elif finished == PROC_MUST_KEEP:
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
        
    def open_ref(self, ref, accept_string=False, make_sweetheart=False):
        """Fetches a reference if it is available, and returns a filename for reading it.
        Options to do with eagerness, streaming, etc.
        If reference is unavailable, raises a ReferenceUnavailableException."""
        ref = self.task_record.retrieve_ref(ref)
        if not accept_string:   
            ctx = retrieve_filename_for_ref(ref, return_ctx=True)
        else:
            ctx = retrieve_file_or_string_for_ref(ref)
        if ctx.completed_ref is not None:
            if make_sweetheart:
                ctx.completed_ref = SW2_SweetheartReference.from_concrete(ctx.completed_ref, get_own_netloc())
            self.task_record.publish_ref(ctx.completed_ref)
        return ctx.to_safe_dict()
        
    def publish_fetched_ref(self, fetch):
        completed_ref = fetch.get_completed_ref()
        if completed_ref is None:
            ciel.log("Cancelling async fetch %s (chunk %d)" % (fetch.ref.id, fetch.chunk_size), "EXEC", logging.INFO)
        else:
            if fetch.make_sweetheart:
                completed_ref = SW2_SweetheartReference.from_concrete(completed_ref, get_own_netloc())
            self.task_record.publish_ref(completed_ref)
        
    def open_ref_async(self, ref, chunk_size, sole_consumer=False, make_sweetheart=False, must_block=False):
        real_ref = self.task_record.retrieve_ref(ref)
        new_fetch = OngoingFetch(real_ref, chunk_size, sole_consumer, make_sweetheart, must_block)
        filename, file_blocking = new_fetch.get_filename()
        if not new_fetch.done:
            self.context_manager.add_context(new_fetch)
            self.ongoing_fetches.append(new_fetch)
        else:
            self.publish_fetched_ref(new_fetch)
        # Definitions here: "done" means we're already certain that the producer has completed successfully.
        # "blocking" means that EOF, as and when it arrives, means what it says. i.e. it's a regular file and done, or a pipe-like thing.
        ret = {"filename": filename, "done": new_fetch.done, "blocking": file_blocking, "size": new_fetch.bytes}
        #ciel.log("Async fetch %s (chunk %d): initial status %d bytes, done=%s, blocking=%s" % (real_ref, chunk_size, ret["size"], ret["done"], ret["blocking"]), "EXEC", logging.INFO)
        if new_fetch.done:
            if not new_fetch.success:
                ciel.log("Async fetch %s failed early" % ref, "EXEC", logging.WARNING)
                ret["error"] = "EFAILED"
        return ret
    
    def close_async_file(self, id, chunk_size):
        for fetch in self.ongoing_fetches:
            if fetch.ref.id == id and fetch.chunk_size == chunk_size:
                self.publish_fetched_ref(fetch)
                self.context_manager.remove_context(fetch)
                self.ongoing_fetches.remove(fetch)
                return
        #ciel.log("Ignored cancel for async fetch %s (chunk %d): not in progress" % (id, chunk_size), "EXEC", logging.WARNING)

    def wait_async_file(self, id, eof=None, bytes=None):
        the_fetch = None
        for fetch in self.ongoing_fetches:
            if fetch.ref.id == id:
                the_fetch = fetch
                break
        if the_fetch is None:
            ciel.log("Failed to wait for async-fetch %s: not an active transfer" % id, "EXEC", logging.WARNING)
            return {"success": False}
        if eof is not None:
            ciel.log("Waiting for fetch %s to complete" % id, "EXEC", logging.INFO)
            the_fetch.wait_eof()
        else:
            ciel.log("Waiting for fetch %s length to exceed %d bytes" % (id, bytes), "EXEC", logging.INFO)
            the_fetch.wait_bytes(bytes)
        if the_fetch.done and not the_fetch.success:
            ciel.log("Wait %s complete: transfer has failed" % id, "EXEC", logging.WARNING)
            return {"success": False}
        else:
            ret = {"size": int(the_fetch.bytes), "done": the_fetch.done, "success": True}
            ciel.log("Wait %s complete: new length=%d, EOF=%s" % (id, ret["size"], ret["done"]), "EXEC", logging.INFO)
            return ret
        
    def spawn(self, request_args):
        """Spawns a child task. Arguments define a task_private structure. Returns a list
        of future references."""
        
        # Args dict arrives from sw with unicode keys :(
        str_args = dict([(str(k), v) for (k, v) in request_args.items()])
        
        if "small_task" not in str_args:
            str_args['small_task'] = False
        
        return spawn_task_helper(self.task_record, **str_args)
    
    def tail_spawn(self, request_args):
        
        if request_args.get("is_fixed", False):
            request_args["process_record_id"] = self.process_record.id
        request_args["delegated_outputs"] = self.task_descriptor["expected_outputs"]
        self.spawn(request_args)
    
    def allocate_output(self, prefix=""):
        new_output_name = self.task_record.create_published_output_name(prefix)
        self.expected_outputs.append(new_output_name)
        return {"index": len(self.expected_outputs) - 1}
    
    def publish_string(self, index, str):
        """Defines a reference with the given string contents."""
        ref = ref_from_safe_string(str, self.expected_outputs[index])
        self.task_record.publish_ref(ref)
        return {"ref": ref}

    def open_output(self, index, may_pipe=False, may_stream=False, make_local_sweetheart=False, can_smart_subscribe=False):
        if may_pipe and not may_stream:
            raise Exception("Insane parameters: may_stream=False and may_pipe=True may well lead to deadlock")
        if index in self.ongoing_outputs:
            raise Exception("Tried to open output %d which was already open" % index)
        output_name = self.expected_outputs[index]
        output_ctx = OngoingOutput(output_name, index, can_smart_subscribe, may_pipe, make_local_sweetheart, self)
        self.ongoing_outputs[index] = output_ctx
        self.context_manager.add_context(output_ctx)
        if may_stream:
            ref = output_ctx.get_stream_ref()
            self.task_record.prepublish_refs([ref])
        filename = output_ctx.get_filename()
        return {"filename": filename}

    def stop_output(self, index):
        self.context_manager.remove_context(self.ongoing_outputs[index])
        del self.ongoing_outputs[index]

    def close_output(self, index, size=None):
        output = self.ongoing_outputs[index]
        if size is None:
            size = output.get_size()
        output.size_update(size)
        self.stop_output(index)
        ret_ref = output.get_completed_ref()
        self.task_record.publish_ref(ret_ref)
        return {"ref": ret_ref}

    def log(self, message):
        t = datetime.now()
        timestamp = time.mktime(t.timetuple()) + t.microsecond / 1e6
        self.worker.master_proxy.log(self.task_descriptor["job"], self.task_descriptor["task_id"], timestamp, message)

    def rollback_output(self, index):
        self.ongoing_outputs[index].rollback()
        self.stop_output(index)

    def output_size_update(self, index, size):
        self.ongoing_outputs[index].size_update(size)

    def _subscribe_output(self, index, chunk_size):
        message = ("subscribe", {"index": index, "chunk_size": chunk_size})
        with self.transmit_lock:
            write_framed_json(message, self.writer)

    def _unsubscribe_output(self, index):
        message = ("unsubscribe", {"index": index})
        with self.transmit_lock:
            write_framed_json(message, self.writer)
           
    def json_event_loop(self, reader, writer):
        while True:

            try:
                (method, args) = read_framed_json(reader)
            except:
                ciel.log('Error reading in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
                
            #ciel.log('Method is %s' % repr(method), 'PROC', logging.INFO)
            response = None
            
            try:
                if method == 'open_ref':
                    
                    if "ref" not in args:
                        ciel.log('Missing required argument key: ref', 'PROC', logging.ERROR, False)
                        return PROC_ERROR
                    
                    try:
                        response = self.open_ref(**args)
                    except ReferenceUnavailableException:
                        response = {'error' : 'EWOULDBLOCK'}
                        
                elif method == "open_ref_async":
                    
                    if "ref" not in args or "chunk_size" not in args:
                        ciel.log("Missing required argument key: open_ref_async needs both 'ref' and 'chunk_size'", "PROC", logging.ERROR, False)
                        return PROC_ERROR
            
                    try:
                        response = self.open_ref_async(**args)
                    except ReferenceUnavailableException:
                        response = {"error": "EWOULDBLOCK"}
                        
                elif method == "wait_stream":
                    response = self.wait_async_file(**args)
                    
                elif method == "close_stream":
                    self.close_async_file(args["id"], args["chunk_size"])
                    
                elif method == 'spawn':
                    
                    response = self.spawn(args)
                                        
                elif method == 'tail_spawn':
                    
                    response = self.tail_spawn(args)
                    
                elif method == 'allocate_output':
                    
                    response = self.allocate_output(**args)
                    
                elif method == 'publish_string':
                    
                    response = self.publish_string(**args)

                elif method == 'log':
                    # No response.
                    self.log(**args)

                elif method == 'open_output':
                    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    
                    response = self.open_output(**args)
                        
                elif method == 'close_output':
    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    response = self.close_output(**args)
                    
                elif method == 'rollback_output':
    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    response = {'ref' : self.rollback_output(**args)}
                    
                elif method == "advert":
                    
                    self.output_size_update(**args)
    
                elif method == "package_lookup":
                    
                    response = {"value": package_lookup(self.task_record, self.block_store, args["key"])}
    
                elif method == 'error':
                    ciel.log('Got error from task: %s' % args["report"], 'PROC', logging.ERROR, False)
                    return PROC_ERROR
    
                elif method == 'exit':
                    
                    if args["keep_process"] == "must_keep":
                        return PROC_MUST_KEEP
                    elif args["keep_process"] == "may_keep":
                        self.soft_cache_keys = args.get("soft_cache_keys", [])
                        return PROC_MAY_KEEP
                    elif args["keep_process"] == "no":
                        return PROC_EXITED
                    else:
                        ciel.log("Bad exit status from task: %s" % args, "PROC", logging.ERROR)
                
                else:
                    ciel.log('Invalid method: %s' % method, 'PROC', logging.WARN, False)
                    return PROC_ERROR

            except:
                ciel.log('Error during method handling in JSON event loop', 'PROC', logging.ERROR, True)
                return PROC_ERROR
        
            try:
                if response is not None:
                    with self.transmit_lock:
                        write_framed_json((method, response), writer)
            except:
                ciel.log('Error writing response in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
        
        return True
    
   
class SkyPyExecutor(ProcExecutor):

    handler_name = "skypy"
    skypybase = os.getenv("CIEL_SKYPY_BASE", None)
    process_cache = set()
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, pyfile_ref=None, coro_ref=None, entry_point=None, entry_args=None, export_json=False, run_fixed=False, is_tail_spawn=False, n_extra_outputs=0, **kwargs):

        if pyfile_ref is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All SkyPy invocations must specify a .py file reference as 'pyfile_ref' or else reference a fixed process")
        if coro_ref is None and (entry_point is None or entry_args is None) and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All SkyPy invocations must specify either coro_ref or entry_point and entry_args, or else reference a fixed process")

        if not is_tail_spawn:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"].append(ret_output)
            task_descriptor["task_private"]["ret_output"] = 0
            extra_outputs = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(n_extra_outputs)]
            task_descriptor["expected_outputs"].extend(extra_outputs)
            task_descriptor["task_private"]["extra_outputs"] = range(1, n_extra_outputs + 1)
        
        if coro_ref is not None:
            task_descriptor["task_private"]["coro_ref"] = coro_ref
            task_descriptor["dependencies"].append(coro_ref)
        else:
            task_descriptor["task_private"]["entry_point"] = entry_point
            task_descriptor["task_private"]["entry_args"] = entry_args
        if not is_tail_spawn:
            task_descriptor["task_private"]["export_json"] = export_json
            task_descriptor["task_private"]["run_fixed"] = run_fixed
        task_descriptor["task_private"]["is_continuation"] = is_tail_spawn
        if pyfile_ref is not None:
            task_descriptor["task_private"]["py_ref"] = pyfile_ref
            task_descriptor["dependencies"].append(pyfile_ref)
        add_package_dep(parent_task_record.package_ref, task_descriptor)

        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record,  
                                                    is_tail_spawn=is_tail_spawn, **kwargs)

    def get_command(self):
        return ["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py")]
        
    def get_env(self):
        return {"PYTHONPATH": SkyPyExecutor.skypybase + ":" + os.environ["PYTHONPATH"]}

    @staticmethod
    def can_run():
        if SkyPyExecutor.skypybase is None:
            ciel.log.error("Can't run SkyPy: CIEL_SKYPY_BASE not in environment", "SKYPY", logging.WARNING)
            return False
        else:
            return test_program(["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py"), "--version"], "PyPy")
   
class Java2Executor(ProcExecutor):
    
    handler_name = "java2"
    process_cache = set()
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "class_name" not in args and "object_ref" not in args:
            raise BlameUserException("All Java2 invocations must specify either a class_name or an object_ref")
        if "jar_lib" not in args:
            raise BlameUserException("All Java2 invocations must specify a jar_lib")
            
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, jar_lib=None, args=None, class_name=None, object_ref=None, n_outputs=1, is_tail_spawn=False, **kwargs):
        # More good stuff goes here.
        if jar_lib is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All Java2 invocations must either specify jar libs or an existing process ID")
        if class_name is None and object_ref is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All Java2 invocations must specify either a class_name or an object_ref, or else give a process ID")
        
        if jar_lib is not None:
            task_descriptor["task_private"]["jar_lib"] = jar_lib
            for jar_ref in jar_lib:
                task_descriptor["dependencies"].append(jar_ref)

        if not is_tail_spawn:
            sha = hashlib.sha1()
            hash_update_with_structure(sha, [args, n_outputs])
            hash_update_with_structure(sha, class_name)
            hash_update_with_structure(sha, object_ref)
            hash_update_with_structure(sha, jar_lib)
            name_prefix = "java2:%s:" % (sha.hexdigest())
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]            
        
        if class_name is not None:
            task_descriptor["task_private"]["class_name"] = class_name
        if object_ref is not None:
            task_descriptor["task_private"]["object_ref"] = object_ref
            task_descriptor["dependencies"].append(object_ref)
        if args is not None:
            task_descriptor["task_private"]["args"] = args
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, n_extra_outputs=0, is_tail_spawn=is_tail_spawn, accept_ref_list_for_single=True, **kwargs)
        
    def get_command(self):
        return ["java", "-Xmx2048M", "-cp", os.getenv('CLASSPATH'), "com.asgow.ciel.executor.Java2Executor"]

    @staticmethod
    def can_run():
        cp = os.getenv("CLASSPATH")
        if cp is None:
            ciel.log.error("Can't run Java: no CLASSPATH set", "JAVA", logging.WARNING)
            return False
        else:
            return test_program(["java", "-cp", cp, "com.asgow.ciel.executor.Java2Executor", "--version"], "Java")

class OCamlExecutor(ProcExecutor):
    
    handler_name = "ocaml"
    process_cache = set()
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "binary" not in args:
            raise BlameUserException("All OCaml invocations must specify a binary")
            
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, binary, args=None, n_outputs=1, is_tail_spawn=False, **kwargs):
        if binary is None:
            raise BlameUserException("All OCaml invocations must specify a binary")
        
        task_descriptor["task_private"]["binary"] = binary

        if not is_tail_spawn:
            sha = hashlib.sha1()
            hash_update_with_structure(sha, [args, n_outputs])
            hash_update_with_structure(sha, binary)
            name_prefix = "ocaml:%s:" % (sha.hexdigest())
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]            
        
        if args is not None:
            task_descriptor["task_private"]["args"] = args
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, n_extra_outputs=0, is_tail_spawn=is_tail_spawn, is_fixed=False, accept_ref_list_for_single=True, **kwargs)
        
    def get_command(self):
        return ["ocaml-wrapper"]

    @staticmethod
    def can_run():
        return test_program(["ocamlc", "-where"], "OCaml")

class SkywritingExecutor(ProcExecutor):

    handler_name = "swi"
    stdlibbase = os.getenv("CIEL_SW_STDLIB", None)
    sw_interpreter_base = os.getenv("CIEL_SW_BASE", None)
    process_cache = set()

    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, sw_file_ref=None, start_env=None, start_args=None, cont_ref=None, n_extra_outputs=0, is_tail_spawn=False, **kwargs):

        if sw_file_ref is None and cont_ref is None:
            raise BlameUserException("Skywriting tasks must specify either a continuation object or a .sw file")
        if n_extra_outputs > 0:
            raise BlameUserException("Skywriting can't deal with extra outputs")

        if not is_tail_spawn:
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
            task_descriptor["task_private"]["ret_output"] = 0

        if cont_ref is not None:
            task_descriptor["task_private"]["cont"] = cont_ref
            task_descriptor["dependencies"].append(cont_ref)
        else:
            # External call: SW file should be started from the beginning.
            task_descriptor["task_private"]["swfile_ref"] = sw_file_ref
            task_descriptor["dependencies"].append(sw_file_ref)
            task_descriptor["task_private"]["start_env"] = start_env
            task_descriptor["task_private"]["start_args"] = start_args
        add_package_dep(parent_task_record.package_ref, task_descriptor)
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, 
                                                  is_tail_spawn=is_tail_spawn, is_fixed=False, **kwargs)

    def get_command(self):
        command = ["python", os.path.join(SkywritingExecutor.sw_interpreter_base, "interpreter_main.py")]
        if SkywritingExecutor.stdlibbase is not None:
            command.extend(["--stdlib-base", SkywritingExecutor.stdlibbase])
        return command

    @staticmethod
    def can_run():
        if SkywritingExecutor.sw_interpreter_base is None:
            ciel.log.error("Can't run Skywriting: CIEL_SW_BASE not in environment", "SKYWRITING", logging.WARNING)
            return False
        else:
            return test_program(["python", os.path.join(SkywritingExecutor.sw_interpreter_base, "interpreter_main.py"), "--version"], "Skywriting")

class SimpleExecutor(BaseExecutor):

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, args, n_outputs, is_tail_spawn=False):

        if is_tail_spawn and len(task_descriptor["expected_outputs"]) != n_outputs:
            raise BlameUserException("SimpleExecutor being built with delegated outputs %s but n_outputs=%d" % (task_descriptor["expected_outputs"], n_outputs))

        # Throw early if the args are bad
        cls.check_args_valid(args, n_outputs)

        # Discover required ref IDs for this executor
        reqd_refs = cls.get_required_refs(args)
        task_descriptor["dependencies"].extend(reqd_refs)

        sha = hashlib.sha1()
        hash_update_with_structure(sha, [args, n_outputs])
        name_prefix = "%s:%s:" % (cls.handler_name, sha.hexdigest())

        # Name our outputs
        if not is_tail_spawn:
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]

        # Add the args dict
        args_name = "%ssimple_exec_args" % name_prefix
        args_ref = ref_from_object(args, "pickle", args_name)
        parent_task_record.publish_ref(args_ref)
        task_descriptor["dependencies"].append(args_ref)
        task_descriptor["task_private"]["simple_exec_args"] = args_ref
        
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record)

        if is_tail_spawn:
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
        self.args = retrieve_object_for_ref(task_private["simple_exec_args"], "pickle")

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

class ProcessRunningExecutor(SimpleExecutor):

    def __init__(self, worker):
        SimpleExecutor.__init__(self, worker)

        self._lock = threading.Lock()
        self.proc = None
        self.context_mgr = None

    def _execute(self):
        self.context_mgr = ContextManager("Simple Task %s" % self.task_id)
        with self.context_mgr:
            self.guarded_execute()

    def guarded_execute(self):
        try:
            self.input_refs = self.args['inputs']
        except KeyError:
            self.input_refs = []
        try:
            self.stream_output = self.args['stream_output']
        except KeyError:
            self.stream_output = False
        try:
            self.pipe_output = self.args['pipe_output']
        except KeyError:
            self.pipe_output = False
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

        file_inputs = None
        push_threads = None

        if self.eager_fetch:
            file_inputs = retrieve_filenames_for_refs(self.input_refs)
        else:
            push_threads = [OngoingFetch(ref, chunk_size=67108864, must_block=True) for ref in self.input_refs]
            for thread in push_threads:
                self.context_mgr.add_context(thread)

        # TODO: Make these use OngoingOutputs and the context manager.                
        with list_with([make_local_output(id, may_pipe=self.pipe_output) for id in self.output_ids]) as out_file_contexts:

            if self.stream_output:
       
                stream_refs = [ctx.get_stream_ref() for ctx in out_file_contexts]
                self.task_record.prepublish_refs(stream_refs)

            # We do these last, as these are the calls which can lead to stalls whilst we await a stream's beginning or end.
            if file_inputs is None:
                file_inputs = []
                for thread in push_threads:
                    (filename, is_blocking) = thread.get_filename()
                    if is_blocking is not None:
                        assert is_blocking is True
                    file_inputs.append(filename)
            
            file_outputs = [filename for (filename, is_fd) in (ctx.get_filename_or_fd() for ctx in out_file_contexts)]
            
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

        self.jar_filenames = retrieve_filenames_for_refs(self.jar_refs)

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
        self.dll_filenames = retrieve_filenames_for_refs(self.dll_refs)

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
        self.so_filenames = retrieve_filenames_for_refs(self.so_refs)

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
            ref = get_ref_for_url(url, version, self.task_id)
            self.task_record.publish_ref(ref)
            out_str = simplejson.dumps(ref, cls=SWReferenceJSONEncoder)
            cache_object(ref, "json", self.output_ids[i])
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
        self.output_refs[0] = ref_from_object(reflist, "json", self.output_ids[0])

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
