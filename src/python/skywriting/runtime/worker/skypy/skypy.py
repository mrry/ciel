
from __future__ import with_statement

import stackless
import traceback
import pickle
import simplejson
from contextlib import closing
from StringIO import StringIO

from shared.io_helpers import MaybeFile
from shared.references import encode_datavalue, decode_datavalue_string,\
    json_decode_object_hook

from file_outputs import OutputFile
from ref_fetch import CompleteFile, StreamingFile

### Constants

HALT_REFERENCE_UNAVAILABLE = 1
HALT_DONE = 2
HALT_RUNTIME_EXCEPTION = 3

### Helpers

class PersistentState:
    def __init__(self, export_json, extra_outputs, py_ref, is_fixed):
        self.export_json = export_json
        self.extra_outputs = extra_outputs
        self.py_ref = py_ref
        self.is_fixed = is_fixed
        self.ref_dependencies = dict()

class ResumeState:
    
    def __init__(self, pstate, coro):
        self.coro = coro
        self.persistent_state = pstate
        
class PackageKeyError(Exception):
    def __init__(self, key):
        Exception.__init__(self)
        self.key = key
        
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
        return current_task.message_helper.synchronous_request("publish_string", args)["ref"]
    
def start_script(entry_point, entry_args):

    try:
        current_task.script_return_val = entry_point(*entry_args)
        current_task.halt_reason = HALT_DONE
    except CoroutineExit:
        # This coroutine is garbage; swallow this quietly
        return
    except Exception, e:
        current_task.script_return_val = e
        current_task.script_backtrace = traceback.format_exc()
        current_task.halt_reason = HALT_RUNTIME_EXCEPTION
        
    current_task.main_coro.switch()

### Task state

current_task = None

class SkyPyTask:
    
    def __init__(self, main_coro, persistent_state, message_helper, file_outputs):
        
        self.main_coro = main_coro
        self.persistent_state = persistent_state
        self.message_helper = message_helper
        self.file_outputs = file_outputs
        self.ref_cache = dict()
        self.script_return_val = None
        self.script_backtrace = None
        self.main_coro_callback_response = None
        self.main_coro_callback = None
        self.halt_reason = 0

def fetch_ref(ref, verb, message_helper, **kwargs):

    send_dict = {"ref": ref}
    send_dict.update(kwargs)
    return message_helper.synchronous_request(verb, send_dict)

def try_fetch_ref(ref, verb, **kwargs):

    if ref.id in current_task.ref_cache:
        return current_task.ref_cache[ref.id]
    else:
        for tries in range(2):
            add_ref_dependency(ref)
            runtime_response = fetch_ref(ref, verb, current_task.message_helper, **kwargs)
            if "error" in runtime_response:
                if tries == 0:
                    current_task.halt_reason = HALT_REFERENCE_UNAVAILABLE
                    current_task.main_coro.switch()
                    continue
                else:
                    raise Exception("Double failure trying to deref %s" % ref.id)
            remove_ref_dependency(ref)
            # We're back -- the ref should be available now.
            return runtime_response

def deref_decode(ref, decode_string_callback, decode_file_callback, **kwargs):
    
    runtime_response = try_fetch_ref(ref, "open_ref", accept_string=True, **kwargs)
    try:
        obj = decode_string_callback(decode_datavalue_string(runtime_response["strdata"]))
    except KeyError:
        with open(runtime_response["filename"], "r") as ref_fp:
            obj = decode_file_callback(ref_fp)
    current_task.ref_cache[ref.id] = obj
    return obj


def deref_json(ref, make_sweetheart=False):
    return deref_decode(ref, lambda x: simplejson.loads(x, object_hook=json_decode_object_hook), 
                            lambda x: simplejson.load(x, object_hook=json_decode_object_hook),
                            make_sweetheart=make_sweetheart)

def deref(ref, make_sweetheart=False):
    return deref_decode(ref, pickle.loads, pickle.load, make_sweetheart=make_sweetheart)

def add_ref_dependency(ref):
    if not ref.is_consumable():
        try:
            current_task.persistent_state.ref_dependencies[ref.id] += 1
        except KeyError:
            current_task.persistent_state.ref_dependencies[ref.id] = 1

def remove_ref_dependency(ref):
    if not ref.is_consumable():
        current_task.persistent_state.ref_dependencies[ref.id] -= 1
        if current_task.persistent_state.ref_dependencies[ref.id] == 0:
            del current_task.persistent_state.ref_dependencies[ref.id]

def save_state(state, make_local_sweetheart=False):

    state_index = get_fresh_output_index(prefix="coro")
    state_fp = MaybeFile(open_callback=lambda: open_output(state_index, make_local_sweetheart=make_local_sweetheart))
    with state_fp:
        pickle.dump(state, state_fp)
    return ref_from_maybe_file(state_fp, state_index)

def do_from_main_coro(callable):
    current_task.main_coro_callback = callable
    current_task.main_coro.switch()
    ret = current_task.main_coro_callback_response
    current_task.main_coro_callback_response = None
    return ret

def create_coroutine(spawn_callable, pargs):
 
    new_coro = stackless.coroutine()
    new_coro.bind(start_script, spawn_callable, pargs)
    return new_coro

def spawn(spawn_callable, *pargs, **kwargs):
    
    new_coro = do_from_main_coro(lambda: create_coroutine(spawn_callable, pargs))
    n_extra_outputs = kwargs.get("n_extra_outputs", 0)
    new_state = PersistentState(export_json=False, 
                                extra_outputs = range(1, n_extra_outputs + 1),
                                py_ref=current_task.persistent_state.py_ref,
                                is_fixed=kwargs.get("run_fixed", False))
    save_obj = ResumeState(new_state, new_coro)
    coro_ref = save_state(save_obj, make_local_sweetheart=True)
    return do_spawn("skypy", False, pyfile_ref=current_task.persistent_state.py_ref, coro_ref=coro_ref, **kwargs)

def do_spawn(executor_name, small_task, **args):
    
    args["small_task"] = small_task
    args["executor_name"] = executor_name
    response = current_task.message_helper.synchronous_request("spawn", args)
    return response

def spawn_exec(executor_name, **args):
    return do_spawn(executor_name, False, **args)

def sync_exec(executor_name, **args):
    return do_spawn(executor_name, True, **args)

def package_lookup(key):
    
    response = current_task.message_helper.synchronous_request("package_lookup", {"key": key})
    retval = response["value"]
    if retval is None:
        raise PackageKeyError(key)
    return retval

def deref_as_raw_file(ref, may_stream=False, sole_consumer=False, chunk_size=67108864, make_sweetheart=False, must_block=False):
    if not may_stream:
        runtime_response = try_fetch_ref(ref, "open_ref", make_sweetheart=make_sweetheart)
        try:
            return closing(StringIO(decode_datavalue_string(runtime_response["strdata"])))
        except KeyError:
            return CompleteFile(ref, runtime_response["filename"])
    else:
        runtime_response = try_fetch_ref(ref, "open_ref_async", chunk_size=chunk_size, sole_consumer=sole_consumer, make_sweetheart=make_sweetheart, must_block=must_block)
        if runtime_response["done"]:
            return CompleteFile(ref, runtime_response["filename"])
        elif runtime_response["blocking"]:
            return CompleteFile(ref, runtime_response["filename"], chunk_size=chunk_size, must_close=True)
        else:
            return StreamingFile(ref, runtime_response["filename"], runtime_response["size"], chunk_size)

def get_fresh_output_index(prefix=""):
    runtime_response = current_task.message_helper.synchronous_request("allocate_output", {"prefix": prefix})
    return runtime_response["index"]

def open_output(index, may_stream=False, may_pipe=False, make_local_sweetheart=False):
    new_output = OutputFile(current_task.message_helper, current_task.file_outputs, index)
    runtime_response = current_task.message_helper.synchronous_request("open_output", {"index": index, "may_stream": may_stream, "may_pipe": may_pipe, "make_local_sweetheart": make_local_sweetheart, "can_smart_subscribe": True})
    new_output.set_filename(runtime_response["filename"])
    return new_output

def get_ret_output_index():
    return 0

def get_extra_output_indices():
    return current_task.persistent_state.extra_outputs
    
def log(message):
    current_task.send_message("log", {"message" : message})
    
class RequiredRefs():
    def __init__(self, refs):
        self.refs = refs

    def __enter__(self):
        for ref in self.refs:
            add_ref_dependency(ref)

    def __exit__(self, x, y, z):
        if x is not CoroutineExit:
            for ref in self.refs:
                remove_ref_dependency(ref)
        return False

    
