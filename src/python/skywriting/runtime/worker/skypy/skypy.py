import stackless
import pickle
import hashlib
import tempfile
import traceback
import os
from StringIO import StringIO

import shared.references
from shared.references import SW2_FutureReference

# Changes from run to run; set externally
main_coro = None
runtime_out = None
runtime_in = None
persistent_state = None
taskid = None

# Volatile; emptied each run
ref_cache = dict()

# Indirect communication with main_coro
script_return_val = None
script_backtrace = None
halt_spawn_id = None
halt_reason = 0
HALT_REFERENCE_UNAVAILABLE = 1
HALT_DONE = 2
HALT_RUNTIME_EXCEPTION = 3

class MaybeFile:

    def __init__(self):
        self.real_fp = None
        self.filename = None
        self.bytes_written = 0
        self.fake_fp = StringIO()

    def write(self, str):
        if self.real_fp is not None:
            self.real_fp.write(str)
        else:
            if self.bytes_written + len(str) > 1024:
                self.fd, self.filename = tempfile.mkstemp()
                self.real_fp = os.fdopen(self.fd, "w")
                self.real_fp.write(self.fake_fp.getvalue())
                self.real_fp.write(str)
                self.fake_fp.close()
            else:
                self.bytes_written += len(str)
                self.fake_fp.write(str)

def describe_maybe_file(output_fp, out_dict):
    if output_fp.real_fp is not None:
        out_dict["filename"] = output_fp.filename
        output_fp.real_fp.close()
    else:
        out_dict["outstr"] = output_fp.fake_fp.getvalue()

class PersistentState:
    def __init__(self):
        self.spawn_counter = 0
        self.ref_dependencies = set()

class ResumeState:
    
    def __init__(self, pstate, coro):
        self.coro = coro
        self.persistent_state = pstate

def create_spawned_task_name():
    sha = hashlib.sha1()
    sha.update('%s:%d' % (taskid, persistent_state.spawn_counter))
    ret = sha.hexdigest()
    persistent_state.spawn_counter += 1
    return ret
    
def create_spawn_output_name(task_id):
    return 'skypy:%s' % task_id

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
                    halt_spawn_id = create_spawned_task_name()
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
                    halt_spawn_id = create_spawned_task_name()
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
    new_coro_fp = MaybeFile()
    pickle.dump(save_obj, new_coro_fp)
    new_task_id = create_spawned_task_name()
    output_id = create_spawn_output_name(new_task_id)
    out_dict = {"request": "spawn", 
                "new_task_id": new_task_id, 
                "output_id": output_id}
    describe_maybe_file(new_coro_fp, out_dict)
    pickle.dump(out_dict, runtime_out)
    return SW2_FutureReference(output_id)

def hash_update_with_structure(hash, value):
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
    else:
        hash.update(str(value))

def get_prefix_for_exec(executor_name, real_args, num_outputs):
    sha = hashlib.sha1()
    hash_update_with_structure(sha, [real_args, num_outputs])
    return '%s:%s:' % (executor_name, sha.hexdigest())
    
def get_names_for_exec(prefix, num_outputs):
    return ['%s%d' % (prefix, i) for i in range(num_outputs)]

def spawn_exec(exec_name, exec_args_dict, n_outputs):

    new_task_id = create_spawned_task_name()
    exec_prefix = get_prefix_for_exec(exec_name, exec_args_dict, n_outputs)
    expected_output_ids = get_names_for_exec(exec_prefix, n_outputs)
    pickle.dump({"request": "spawn_exec",
                 "args": exec_args_dict,
                 "new_task_id": new_task_id,
                 "executor_name": exec_name,
                 "exec_prefix": exec_prefix,
                 "output_ids": expected_output_ids}, 
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
                halt_spawn_id = create_spawned_task_name()
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
    
