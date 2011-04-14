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
    SW2_FixedReference, SWReferenceJSONEncoder
from shared.io_helpers import read_framed_json, write_framed_json
from skywriting.runtime.exceptions import BlameUserException, MissingInputException
from skywriting.runtime.executor_helpers import ContextManager, retrieve_filename_for_ref, \
    retrieve_filenames_for_refs, get_ref_for_url, ref_from_string, \
    retrieve_file_or_string_for_ref, ref_from_safe_string,\
    write_fixed_ref_string
from skywriting.runtime.block_store import get_own_netloc

from skywriting.runtime.producer import make_local_output
from skywriting.runtime.fetcher import fetch_ref_async
from skywriting.runtime.object_cache import retrieve_object_for_ref, ref_from_object

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

    def __init__(self, ref, chunk_size, sole_consumer):
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.bytes = 0
        self.ref = ref
        self.chunk_size = chunk_size
        self.sole_consumer = sole_consumer
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
            while self.filename is None and self.success is None:
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

    def __init__(self, output_name, output_index, may_pipe, executor):
        self.output_ctx = make_local_output(output_name, subscribe_callback=self.subscribe_output, may_pipe=may_pipe)
        self.output_name = output_name
        self.output_index = output_index
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

    def get_filename(self):
        (filename, is_fd) = self.output_ctx.get_filename_or_fd()
        assert not is_fd
        return filename

    def get_stream_ref(self):
        return self.output_ctx.get_stream_ref()

    def get_completed_ref(self):
        return self.output_ctx.get_completed_ref()

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
PROC_BLOCKED = 1
PROC_ERROR = 2

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
                              process_record_id=None, start_command=None, start_env={}, is_fixed=False,
                              n_extra_outputs=0, extra_dependencies=[], is_tail_spawn=False):

        if process_record_id is None and start_command is None:
            raise BlameUserException("ProcExecutor tasks must specify either process_record_id or start_command")

        if process_record_id is not None:
            task_descriptor["task_private"]["id"] = process_record_id
        else:
            task_descriptor["task_private"]["start_command"] = start_command
            task_descriptor["task_private"]["start_env"] = start_env
        task_descriptor["dependencies"].extend(extra_dependencies)

        if not is_tail_spawn:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"].append(ret_output)
            task_descriptor["task_private"]["ret_output"] = 0
            extra_outputs = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(n_extra_outputs)]
            task_descriptor["expected_outputs"].extend(extra_outputs)
            task_descriptor["task_private"]["extra_outputs"] = range(1, n_extra_outputs + 1)

        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        if is_fixed:     
            task_private_ref = SW2_FixedReference(task_private_id, get_own_netloc())
            write_fixed_ref_string(pickle.dumps(task_descriptor["task_private"]), task_private_ref)
        else:
            task_private_ref = ref_from_string(pickle.dumps(task_descriptor["task_private"]), task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)
        
        if not is_tail_spawn:
            if len(task_descriptor["expected_outputs"]) == 1:
                return SW2_FutureReference(task_descriptor["expected_outputs"][0])
            else:
                return [SW2_FutureReference(refid) for refid in task_descriptor["expected_outputs"]]

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
            self.process_record = self.process_pool.create_process_record(None, "json")
            command = task_private["start_command"]
            command.extend(["--write-fifo", self.process_record.get_read_fifo_name(), 
                            "--read-fifo", self.process_record.get_write_fifo_name()])
            new_proc_env = os.environ.copy()
            new_proc_env.update(task_private["start_env"])
            new_proc = subprocess.Popen(command, env=new_proc_env, close_fds=True)
            self.process_record.set_pid(new_proc.pid)
               
        # XXX: This will block until the attached process opens the pipes.
        reader = self.process_record.get_read_fifo()
        writer = self.process_record.get_write_fifo()
        self.reader = reader
        self.writer = writer
        
        ciel.log('Got reader and writer FIFOs', 'PROC', logging.INFO)

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
        
    def open_ref(self, ref, accept_string=False):
        """Fetches a reference if it is available, and returns a filename for reading it.
        Options to do with eagerness, streaming, etc.
        If reference is unavailable, raises a ReferenceUnavailableException."""
        ref = self.task_record.retrieve_ref(ref)
        if not accept_string:   
            return {"filename": retrieve_filename_for_ref(ref)}
        else:
            return retrieve_file_or_string_for_ref(ref).to_safe_dict()
        
    def open_ref_async(self, ref, chunk_size, sole_consumer=False):
        real_ref = self.task_record.retrieve_ref(ref)
        new_fetch = OngoingFetch(real_ref, chunk_size, sole_consumer)
        filename, file_blocking = new_fetch.get_filename()
        if not new_fetch.done:
            self.context_manager.add_context(new_fetch)
            self.ongoing_fetches.append(new_fetch)
        # Definitions here: "done" means we're already certain that the producer has completed successfully.
        # "blocking" means that EOF, as and when it arrives, means what it says. i.e. it's a regular file and done, or a pipe-like thing.
        ret = {"filename": filename, "done": new_fetch.done, "blocking": file_blocking, "size": new_fetch.bytes}
        ciel.log("Async fetch %s (chunk %d): initial status %d bytes, done=%s, blocking=%s" % (real_ref, chunk_size, ret["size"], ret["done"], ret["blocking"]), "EXEC", logging.INFO)
        if new_fetch.done:
            if not new_fetch.success:
                ciel.log("Async fetch %s failed early" % ref, "EXEC", logging.WARNING)
                ret["error"] = "EFAILED"
        return ret
    
    def close_async_file(self, id, chunk_size):
        for fetch in self.ongoing_fetches:
            if fetch.ref.id == id and fetch.chunk_size == chunk_size:
                self.context_manager.remove_context(fetch)
                self.ongoing_fetches.remove(fetch)
                completed_ref = fetch.get_completed_ref()
                if completed_ref is None:
                    ciel.log("Cancelling async fetch %s (chunk %d)" % (id, chunk_size), "EXEC", logging.INFO)
                else:
                    self.task_record.publish_ref(completed_ref)
                return
        ciel.log("Ignored cancel for async fetch %s (chunk %d): not in progress" % (id, chunk_size), "EXEC", logging.WARNING)

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
            ret = {"size": the_fetch.bytes, "done": the_fetch.done, "success": True}
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

    def open_output(self, index, may_pipe=False):
        if index in self.ongoing_outputs:
            raise Exception("Tried to open output %d which was already open" % index)
        output_name = self.expected_outputs[index]
        output_ctx = OngoingOutput(output_name, index, may_pipe, self)
        self.ongoing_outputs[index] = output_ctx
        self.context_manager.add_context(output_ctx)
        ref = output_ctx.get_stream_ref()
        self.task_record.prepublish_refs([ref])
        filename = output_ctx.get_filename()
        return {"filename": filename}

    def stop_output(self, index):
        self.context_manager.remove_context(self.ongoing_outputs[index])
        del self.ongoing_outputs[index]

    def close_output(self, index, size):
        output = self.ongoing_outputs[index]
        output.size_update(size)
        self.stop_output(index)
        ret_ref = output.get_completed_ref()
        self.task_record.publish_ref(ret_ref)
        return {"ref": ret_ref}

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
                
            ciel.log('Method is %s' % repr(method), 'PROC', logging.INFO)
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
                    
                    if args["keep_process"]:
                        return PROC_BLOCKED
                    else:
                        return PROC_EXITED
                
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
    skypybase = os.getenv("CIEL_SKYPY_BASE")
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, pyfile_ref=None, coro_ref=None, entry_point=None, entry_args=None, export_json=False, is_tail_spawn=False, n_extra_outputs=0, **kwargs):

        if pyfile_ref is None:
            raise BlameUserException("All SkyPy invocations must specify a .py file reference as 'pyfile_ref'")
        if coro_ref is None and (entry_point is None or entry_args is None):
            raise BlameUserException("All SkyPy invocations must specify either coro_ref or entry_point and entry_args")
        if coro_ref is not None:
            task_descriptor["task_private"]["coro_ref"] = coro_ref
            task_descriptor["dependencies"].append(coro_ref)
        else:
            task_descriptor["task_private"]["entry_point"] = entry_point
            task_descriptor["task_private"]["entry_args"] = entry_args
        if not is_tail_spawn:
            task_descriptor["task_private"]["export_json"] = export_json
        task_descriptor["task_private"]["is_continuation"] = is_tail_spawn
        task_descriptor["task_private"]["py_ref"] = pyfile_ref
        task_descriptor["dependencies"].append(pyfile_ref)
        add_package_dep(parent_task_record.package_ref, task_descriptor)

        command = ["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py")]
        env = {"PYTHONPATH": SkyPyExecutor.skypybase + ":" + os.environ["PYTHONPATH"]}
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, start_command=command, start_env=env, 
                                                    is_fixed=False, is_tail_spawn=is_tail_spawn, n_extra_outputs=n_extra_outputs, **kwargs)

    @staticmethod
    def can_run():
        if "CIEL_SKYPY_BASE" not in os.environ:
            ciel.log.error("Can't run SkyPy: CIEL_SKYPY_BASE not in environment", "SKYPY", logging.WARNING)
            return False
        else:
            return test_program(["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py"), "--version"], "PyPy")
   
class Java2Executor(ProcExecutor):
    
    handler_name = "java2"
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record):
        # More good stuff goes here.
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record)

    @staticmethod
    def can_run():
        return False
        cp = os.getenv("CLASSPATH")
        if cp is None:
            ciel.log.error("Can't run Java: no CLASSPATH set", "JAVA", logging.WARNING)
            return False
        else:
            return test_program(["java", "-cp", cp, "com.asgow.ciel.executor.Java2Executor", "--version"], "Java")

# Imports for Skywriting

from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    ExecutionInterruption
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
    def build_task_descriptor(cls, task_descriptor, parent_task_record, sw_file_ref=None, start_env=None, start_args=None, cont=None, cont_delegated_output=None, extra_dependencies={}):

        if cont_delegated_output is None:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"] = [ret_output]
        else:
            task_descriptor["expected_outputs"] = [cont_delegated_output]
        if cont is not None:
            cont_id = "%s:cont" % task_descriptor["task_id"]
            spawned_cont_ref = ref_from_object(cont, "pickle", cont_id)
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
        
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record)

        if cont_delegated_output is not None:
            return None
        else:
            return SW2_FutureReference(ret_output)

    def start_sw_script(self, swref, args, env):

        sw_file = retrieve_filename_for_ref(swref)
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
            self.continuation = retrieve_object_for_ref(sw_private["cont"], 'pickle')
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

            result_ref = ref_from_object(result, "json", task_descriptor["expected_outputs"][0])
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
        ret = retrieve_object_for_ref(real_ref, "json")
        self.lazy_derefs.discard(ref)
        return ret

    def include_script(self, target_expr):
        if isinstance(target_expr, basestring):
            # Name may be relative to the local stdlib.
            if not target_expr.startswith('http'):
                target_url = 'file://%s' % os.path.join(self.stdlibbase, target_expr)
            else:
                target_url = target_expr
            target_ref = get_ref_for_url(target_url, 0, self.task_id)
        elif isinstance(target_expr, SWRealReference):    
            target_ref = target_expr
        else:
            raise BlameUserException('Invalid object %s passed as the argument of include', 'INCLUDE', logging.ERROR)

        try:
            script = retrieve_object_for_ref(target_ref, 'script')
        except:
            ciel.log.error('Error parsing included script', 'INCLUDE', logging.ERROR, True)
            raise BlameUserException('The included script did not parse successfully')
        return script

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

class AsyncPushThread:

    def __init__(self, ref, chunk_size=67108864):
        self.ref = ref
        self.chunk_size = chunk_size
        self.next_threshold = self.chunk_size
        self.success = None
        self.lock = threading.RLock()
        self.fetch_done = False
        self.stream_done = False
        self.stream_started = False
        self.bytes_copied = 0
        self.bytes_available = 0
        self.completed_ref = None
        self.condvar = threading.Condition(self.lock)
        self.thread = None
        self.read_filename = None
        self.filename = None

    def _check_completion(self):
        if self.success is False:
            ciel.log("Fetch for %s failed" % self.ref, "EXEC", logging.INFO)
            return True
        elif self.success is True:
            ciel.log("Fetch for %s completed; using file directly" % self.ref, "EXEC", logging.INFO)
            return True
        elif self.filename is not None and self.file_blocking:
            ciel.log("Fetch for %s yielded a blocking-readable file; using directly" % self.ref, "EXEC", logging.INFO)
            return True
        else:
            return False

    def check_completion(self):
        ret = self._check_completion()
        if ret:
            with self.lock:
                self.filename = self.read_filename
                self.stream_done = True
                self.condvar.notify_all()
        return ret

    def start(self, fifos_dir):
        self.fifos_dir = fifos_dir
        self.file_fetch = fetch_ref_async(self.ref,
                                          chunk_size=self.chunk_size,
                                          result_callback=self.result, 
                                          reset_callback=self.reset, 
                                          progress_callback=self.progress,
                                          start_filename_callback=self.set_filename)
        if not self.check_completion():
            self.thread = threading.Thread(target=self.thread_main)
            self.thread.start()

    def thread_main(self):

        with self.lock:
            while self.read_filename is None and not self.fetch_done:
                self.condvar.wait()
            if self.check_completion():
                return
            while self.bytes_available < self.next_threshold and not self.fetch_done:
                self.condvar.wait()
            if self.check_completion():
                return
            else:
                self.stream_started = True
        ciel.log("Fetch for %s got more than %d bytes; commencing asynchronous push" % (self.ref, self.chunk_size), "EXEC", logging.INFO)
        self.copy_loop()

    def copy_loop(self):
        
        try:
            fifo_name = os.path.join(self.fifos_dir, "fifo-%s" % self.ref.id)
            os.mkfifo(fifo_name)
            with self.lock:
                self.filename = fifo_name
                self.condvar.notify_all()
            with open(self.read_filename, "r") as input_fp:
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
            ciel.log("Push thread for %s died with exception %s" % (self.read_filename, e), "EXEC", logging.WARNING)
            with self.lock:
                self.stream_done = True
                self.condvar.notify_all()
                
    def get_completed_ref(self, make_sweetheart):
        if make_sweetheart and self.completed_ref is not None:
            return SW2_SweetheartReference(self.completed_ref.id, [get_own_netloc()], self.completed_ref.size_hint, self.completed_ref.netlocs)
        else:
            return self.completed_ref

    def result(self, success, completed_ref):
        with self.lock:
            if self.success is None:
                self.success = success
                self.completed_ref = completed_ref
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

    def set_filename(self, filename, file_blocking):
        with self.lock:
            self.read_filename, self.file_blocking = filename, file_blocking
            self.condvar.notify_all()

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
            while (not self.stream_done) or (self.success is None):
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

        if self.eager_fetch:
            push_threads = [DummyPushThread(x) for x in retrieve_filenames_for_refs(self.input_refs)]
            push_ctx = DummyPushGroup(push_threads)
        else:
            push_threads = [AsyncPushThread(ref) for ref in self.input_refs]
            push_ctx = AsyncPushGroup(push_threads, self.make_sweetheart, self.task_record)

        with push_ctx:
            
            with list_with([make_local_output(id, may_pipe=self.pipe_output) for id in self.output_ids]) as out_file_contexts:

                if self.stream_output:
           
                    stream_refs = [ctx.get_stream_ref() for ctx in out_file_contexts]
                    self.task_record.prepublish_refs(stream_refs)

                # We do these last, as these are the calls which can lead to stalls whilst we await a stream's beginning or end.
                file_inputs = [push_thread.get_filename() for push_thread in push_threads]
                
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
