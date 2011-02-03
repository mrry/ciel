
from __future__ import with_statement

import stackless
import pickle
import hashlib
import tempfile
import traceback
import os
from StringIO import StringIO

import shared.references
from shared.references import SW2_FutureReference
from shared.io_helpers import MaybeFile

# Changes from run to run; set externally
main_coro = None
runtime_out = None
runtime_in = None
persistent_state = None
taskid = None

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
        out_dict["outstr"] = output_fp.fake_fp.getvalue()

class PersistentState:
    def __init__(self):
        self.ref_dependencies = set()

class ResumeState:
    
    def __init__(self, pstate, coro):
        self.coro = coro
        self.persistent_state = pstate

def fetch_ref(ref, verb):

    global halt_reason
    global ref_cache
    global halt_spawn_id
    
    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            pickle.dump({"request": verb, "ref": ref}, runtime_out)
            runtime_out.flush()
            runtime_response = pickle.load(runtime_in)
            if not runtime_response["success"]:
                if tries == 0:
                    halt_reason = HALT_REFERENCE_UNAVAILABLE
                    main_coro.switch()
                    continue
                else:
                    raise Exeception("Double failure trying to deref %s" % ref.id)
            # We're back -- the ref should be available now.
            return runtime_response

def deref_json(ref):
    
    runtime_response = fetch_ref(ref, "deref_json")
    obj = runtime_response["obj"]
    ref_cache[ref.id] = obj
    return obj    

def deref(ref):

    runtime_response = fetch_ref(ref, "deref")
    try:
        obj = pickle.loads(runtime_response["strdata"])
    except KeyError:
        ref_fp = open(runtime_response["filename"], "r")
        obj = pickle.load(ref_fp)
        ref_fp.close()
    ref_cache[ref.id] = obj
    return obj

def add_ref_dependency(ref):
    if not ref.is_consumable():
        persistent_state.ref_dependencies.add(ref)

def remove_ref_dependency(ref):
    if not ref.is_consumable():
        persistent_state.ref_dependencies.remove(ref)

class RequiredRefs():
    def __init__(self, refs):
        self.refs = refs

    def __enter__(self):
        for ref in self.refs:
            add_ref_dependency(ref)

    def __exit__(self, x, y, z):
        for ref in self.refs:
            remove_ref_dependency(ref)

def spawn(spawn_callable, *args):
    
    new_coro = stackless.coroutine()
    new_coro.bind(start_script, spawn_callable, args)
    save_obj = ResumeState(PersistentState(), new_coro)
    with MaybeFile() as new_coro_fp:
        pickle.dump(save_obj, new_coro_fp)
        out_dict = {"request": "spawn"}
        describe_maybe_file(new_coro_fp, out_dict)
    pickle.dump(out_dict, runtime_out)
    runtime_out.flush()
    response = pickle.load(runtime_in)
    return response["output"]

def do_exec(executor_name, small_task, **args):
    
    args["request"] = "exec"
    args["small_task"] = small_task
    args["executor_name"] = executor_name
    pickle.dump(args,
                runtime_out)
    runtime_out.flush()
    return pickle.load(runtime_in)["outputs"]

def spawn_exec(executor_name, **args):
    return do_exec(executor_name, False, **args)

def sync_exec(executor_name, **args):
    return do_exec(executor_name, True, **args)

class PackageKeyError:
    def __init__(self, key):
        self.key = key

def package_lookup(key):
    
    pickle.dump({"request": "package_lookup", "key": key}, runtime_out)
    runtime_out.flush()
    retval = pickle.load(runtime_in)["value"]
    if retval is None:
        raise PackageKeyError(key)
    return retval

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
    
