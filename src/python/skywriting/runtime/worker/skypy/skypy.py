
from __future__ import with_statement

import stackless
import traceback
import pickle
import simplejson
import os
from contextlib import closing
from StringIO import StringIO

from shared.io_helpers import MaybeFile
from shared.references import encode_datavalue, decode_datavalue_string,\
    json_decode_object_hook

from file_outputs import OutputFile

# Changes from run to run; set externally
main_coro = None
persistent_state = None
taskid = None
ret_output = None
other_outputs = None
message_helper = None
file_outputs = None

# Volatile; emptied each run
ref_cache = dict()
spawn_counter = 0

# Indirect communication with main_coro
script_return_val = None
script_backtrace = None
halt_reason = 0
HALT_REFERENCE_UNAVAILABLE = 1
HALT_DONE = 2
HALT_RUNTIME_EXCEPTION = 3

def describe_maybe_file(output_fp, out_dict):
    if output_fp.real_fp is not None:
        out_dict["filename"] = output_fp.filename
        output_fp.real_fp.close()
    else:
        out_dict["strdata"] = encode_datavalue(output_fp.str)
        
def ref_from_maybe_file(output_fp, refidx):
    if output_fp.real_fp is not None:
        return output_fp.real_fp.get_completed_ref()
    else:
        args = {"index": refidx, "str": encode_datavalue(output_fp.str)}
        return message_helper.synchronous_request("publish_string", args)["ref"]

class PersistentState:
    def __init__(self):
        self.ref_dependencies = dict()

class ResumeState:
    
    def __init__(self, pstate, coro):
        self.coro = coro
        self.persistent_state = pstate

def fetch_ref(ref, verb, **kwargs):

    global halt_reason
        
    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            add_ref_dependency(ref)
            send_dict = {"ref": ref}
            send_dict.update(kwargs)
            runtime_response = message_helper.synchronous_request(verb, send_dict)
            if "error" in runtime_response:
                if tries == 0:
                    halt_reason = HALT_REFERENCE_UNAVAILABLE
                    main_coro.switch()
                    continue
                else:
                    raise Exception("Double failure trying to deref %s" % ref.id)
            remove_ref_dependency(ref)
            # We're back -- the ref should be available now.
            return runtime_response

def deref_json(ref):
    
    runtime_response = fetch_ref(ref, "open_ref")
    try:
        obj = simplejson.loads(decode_datavalue_string(runtime_response["strdata"]), object_hook=json_decode_object_hook)
    except KeyError:
        with open(runtime_response["filename"], "r") as ref_fp:
            obj = simplejson.load(ref_fp, object_hook=json_decode_object_hook)
    ref_cache[ref.id] = obj
    return obj

def deref(ref):

    runtime_response = fetch_ref(ref, "open_ref")
    try:
        obj = pickle.loads(decode_datavalue_string(runtime_response["strdata"]))
    except KeyError:
        with open(runtime_response["filename"], "r") as ref_fp:
            obj = pickle.load(ref_fp)
    ref_cache[ref.id] = obj
    return obj

def add_ref_dependency(ref):
    if not ref.is_consumable():
        try:
            persistent_state.ref_dependencies[ref.id] += 1
        except KeyError:
            persistent_state.ref_dependencies[ref.id] = 1

def remove_ref_dependency(ref):
    if not ref.is_consumable():
        persistent_state.ref_dependencies[ref.id] -= 1
        if persistent_state.ref_dependencies[ref.id] == 0:
            del persistent_state.ref_dependencies[ref.id]

class RequiredRefs():
    def __init__(self, refs):
        self.refs = refs

    def __enter__(self):
        for ref in self.refs:
            add_ref_dependency(ref)

    def __exit__(self, x, y, z):
        for ref in self.refs:
            remove_ref_dependency(ref)

def save_state(state):

    state_index = get_fresh_output_index(prefix="coro")
    state_fp = MaybeFile(open_callback=lambda: open_output(state_index))
    with state_fp:
        pickle.dump(state, state_fp)
    return ref_from_maybe_file(state_fp, state_index)

def spawn(spawn_callable, *pargs, **kwargs):
    
    new_coro = stackless.coroutine()
    new_coro.bind(start_script, spawn_callable, pargs)
    save_obj = ResumeState(None, new_coro)
    coro_ref = save_state(save_obj)
    return do_spawn("skypy", False, pyfile_ref=persistent_state.py_ref, coro_ref=coro_ref, **kwargs)

def do_spawn(executor_name, small_task, **args):
    
    args["small_task"] = small_task
    args["executor_name"] = executor_name
    response = message_helper.synchronous_request("spawn", args)
    return response

def spawn_exec(executor_name, **args):
    return do_spawn(executor_name, False, **args)

def sync_exec(executor_name, **args):
    return do_spawn(executor_name, True, **args)

class PackageKeyError(Exception):
    def __init__(self, key):
        Exception.__init__(self)
        self.key = key

def package_lookup(key):
    
    response = message_helper.synchronous_request("package_lookup", {"key": key})
    retval = response["value"]
    if retval is None:
        raise PackageKeyError(key)
    return retval

class CompleteFile:

    def __init__(self, ref, filename, chunk_size=None, must_close=False):
        self.ref = ref
        self.filename = filename
        self.chunk_size = chunk_size
        self.must_close = must_close
        self.fp = open(self.filename, "r")
        add_ref_dependency(self.ref)

    def close(self):
        self.fp.close()
        if self.must_close:
            message_helper.send_message("close_stream", {"id": self.ref.id, "chunk_size": self.chunk_size})            
        remove_ref_dependency(self.ref)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        self.close()

    def __getattr__(self, name):
        return getattr(self.fp, name)

    def __getstate__(self):
        if self.fp.closed:
            return (self.ref, None, None, None)
        else:
            return (self.ref, self.fp.tell(), self.chunk_size, self.must_close)

    def __setstate__(self, (ref, offset, chunk_size, must_close)):
        self.ref = ref
        if offset is not None:
            if must_close is True:
                runtime_response = fetch_ref(self.ref, "open_ref_async", chunk_size=chunk_size)
                self.must_close = runtime_response["blocking"] and not runtime_response["done"]
                self.chunk_size = chunk_size
            else:
                runtime_response = fetch_ref(self.ref, "open_ref")
            self.filename = runtime_response["filename"]
            self.fp = open(self.filename, "r")
            self.fp.seek(offset, os.SEEK_SET)
        # Else this is a closed file object.

class StreamingFile:
    
    def __init__(self, ref, filename, initial_size, chunk_size):
        self.ref = ref
        self.filename = filename
        self.chunk_size = chunk_size
        self.really_eof = False
        self.current_size = None
        self.fp = open(self.filename, "r")
        self.closed = False
        self.softspace = False
        add_ref_dependency(self.ref)

    def __enter__(self):
        return self

    def close(self):
        self.closed = True
        self.fp.close()
        message_helper.send_message("close_stream", {"id": self.ref.id, "chunk_size": self.chunk_size})
        remove_ref_dependency(self.ref)

    def __exit__(self, exnt, exnv, exnbt):
        self.close()

    def wait(self, **kwargs):
        out_dict = {"id": self.ref.id}
        out_dict.update(kwargs)
        runtime_response = message_helper.synchronous_request("wait_stream", out_dict)
        if not runtime_response["success"]:
            raise Exception("File transfer failed before EOF")
        else:
            self.really_eof = runtime_response["done"]
            self.current_size = runtime_response["size"]

    def wait_bytes(self, bytes):
        bytes = self.chunk_size * ((bytes / self.chunk_size) + 1)
        self.wait(bytes=bytes)

    def read(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.read(*pargs)
            if self.really_eof or (bytes is not None and len(ret) == bytes):
                return ret
            else:
                self.fp.seek(-len(ret), os.SEEK_CUR)
                if bytes is None:
                    self.wait(eof=True)
                else:
                    self.wait_bytes(self.fp.tell() + bytes)

    def readline(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.readline(*pargs)
            if self.really_eof or (bytes is not None and len(ret) == bytes) or ret[-1] == "\n":
                return ret
            else:
                self.fp.seek(-len(ret), os.SEEK_CUR)
                # I wait this long whether or not the byte-limit is set in the hopes of finding a \n before then.
                self.wait_bytes(self.fp.tell() + len(ret) + 128)

    def readlines(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.readlines(*pargs)
            bytes_read = 0
            for line in ret:
                bytes_read += len(line)
            if self.really_eof or (bytes is not None and bytes_read == bytes) or ret[-1][-1] == "\n":
                return ret
            else:
                self.fp.seek(-bytes_read, os.SEEK_CUR)
                self.wait_bytes(self.fp.tell() + bytes_read + 128)

    def xreadlines(self):
        return self

    def __iter__(self):
        return self

    def next(self):
        ret = self.readline()
        if ret == "\n":
            raise StopIteration()

    def __getstate__(self):
        if not self.fp.closed:
            return (self.ref, self.fp.tell(), self.chunk_size)
        else:
            return (self.ref, None, self.chunk_size)

    def __setstate__(self, (ref, offset, chunk_size)):
        self.ref = ref
        self.chunk_size = chunk_size
        if offset is not None:
            runtime_response = fetch_ref(self.ref, "open_ref_async", chunk_size=chunk_size)
            self.really_eof = runtime_response["done"]
            self.current_size = runtime_response["size"]
            self.fp = open(runtime_response["filename"], "r")
            self.fp.seek(offset, os.SEEK_SET)
        # Else we're already closed

def deref_as_raw_file(ref, may_stream=False, sole_consumer=False, chunk_size=67108864):
    if not may_stream:
        runtime_response = fetch_ref(ref, "open_ref")
        try:
            return closing(StringIO(decode_datavalue_string(runtime_response["strdata"])))
        except KeyError:
            return CompleteFile(ref, runtime_response["filename"])
    else:
        runtime_response = fetch_ref(ref, "open_ref_async", chunk_size=chunk_size, sole_consumer=sole_consumer)
        if runtime_response["done"]:
            return CompleteFile(ref, runtime_response["filename"])
        elif runtime_response["blocking"]:
            return CompleteFile(ref, runtime_response["filename"], chunk_size=chunk_size, must_close=True)
        else:
            return StreamingFile(ref, runtime_response["filename"], runtime_response["size"], chunk_size)

def get_fresh_output_index(prefix=""):
    runtime_response = message_helper.synchronous_request("allocate_output", {"prefix": prefix})
    return runtime_response["index"]

def open_output(index, may_pipe=False):
    new_output = OutputFile(message_helper, file_outputs, index)
    runtime_response = message_helper.synchronous_request("open_output", {"index": index, "may_pipe": may_pipe})
    new_output.set_filename(runtime_response["filename"])
    return new_output

def start_script(entry_point, entry_args):

    global halt_reason
    global script_return_val
    global script_backtrace

    try:
        script_return_val = entry_point(*entry_args)
        halt_reason = HALT_DONE
    except Exception, e:
        script_return_val = e
        script_backtrace = traceback.format_exc()
        halt_reason = HALT_RUNTIME_EXCEPTION
        
    main_coro.switch()
    
