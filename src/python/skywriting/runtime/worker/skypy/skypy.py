
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
from shared.exec_helpers import get_exec_prefix, get_exec_output_ids
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

def create_spawn_output_name():
    global spawn_counter
    ret = 'skypy:%s:spawnout:%d' % (taskid, spawn_counter)
    spawn_counter += 1
    return ret

def deref(ref):

    global halt_reason
    global ref_cache
    global halt_spawn_id
    
    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            pickle.dump({"request": "deref", "ref": ref}, runtime_out)
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

def deref_json(ref):

    global halt_reason
    global ref_cache
    global halt_spawn_id

    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            pickle.dump({"request": "deref_json", "ref": ref}, runtime_out)
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
            obj = runtime_response["obj"]
            ref_cache[ref.id] = obj
            return obj

def spawn(spawn_callable):
    
    new_coro = stackless.coroutine()
    new_coro.bind(spawn_callable)
    save_obj = ResumeState(PersistentState(), new_coro)
    with MaybeFile() as new_coro_fp:
        pickle.dump(save_obj, new_coro_fp)
        output_id = create_spawn_output_name()
        out_dict = {"request": "spawn", 
                    "output_id": output_id}
        describe_maybe_file(new_coro_fp, out_dict)
    pickle.dump(out_dict, runtime_out)
    return SW2_FutureReference(output_id)

def spawn_exec(exec_name, exec_args_dict, n_outputs):

    exec_prefix = get_exec_prefix(exec_name, exec_args_dict, n_outputs)
    expected_output_ids = get_names_for_exec(exec_prefix, n_outputs)
    pickle.dump({"request": "spawn_exec",
                 "args": exec_args_dict,
                 "executor_name": exec_name,
                 "n_outputs": n_outputs,
                 "exec_prefix": exec_prefix},
                runtime_out)

    return [SW2_FutureReference(id) for id in expected_output_ids]

def sync_exec(exec_name, exec_args_dict, n_outputs):

    global halt_spawn_id
    global halt_reason

    for tries in range(2):

        pickle.dump({"request": "exec",
                     "executor_name": exec_name,
                     "args": exec_args_dict,
                     "n_outputs": n_outputs},
                    runtime_out)
        runtime_out.flush()
        result = pickle.load(runtime_in)
        if result["success"]:
            return result["outputs"]
        else:
            if tries == 0:
                halt_reason = HALT_REFERENCE_UNAVAILABLE
                main_coro.switch()
            else:
                raise Exception("Failed to exec more than once")

def freeze_script_at_startup(entry_point):

    global halt_reason
    global script_return_val
    global script_backtrace

    local_args = initial_run_args
    main_coro.switch()
    try:
        script_return_val = entry_point(*local_args)
        halt_reason = HALT_DONE
    except Exception, e:
        script_return_val = e
        script_backtrace = traceback.format_exc()
        halt_reason = HALT_RUNTIME_EXCEPTION
        
    main_coro.switch()
    
