import stackless
import pickle
import hashlib
import tempfile
import traceback
import os

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

class OpaqueReference:

    def __init__(self, id):
        self.id = id

class PersistentState:
    def __init__(self):
        self.spawn_counter = 0

class ResumeState:
    
    def __init__(self, pstate, coro):
        self.coro = coro
        self.persistent_state = pstate

def map_leaf_values(f, value):
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

def create_spawned_task_name():
    sha = hashlib.sha1()
    sha.update('%s:%d' % (taskid, persistent_state.spawn_counter))
    ret = sha.hexdigest()
    persistent_state.spawn_counter += 1
    return ret
    
def create_spawn_output_name(task_id):
    return 'swi:%s' % task_id

def deref(ref):

    global halt_reason
    global ref_cache
    global halt_spawn_id
    
    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            pickle.dump({"request": "deref", "id": ref.id}, runtime_out)
            runtime_out.flush()
            runtime_response = pickle.load(runtime_in)
            if runtime_response["filename"] is None:
                if tries == 0:
                    halt_spawn_id = create_spawned_task_name()
                    halt_reason = HALT_REFERENCE_UNAVAILABLE
                    main_coro.switch()
                    continue
                else:
                    raise Exeception("Double failure trying to deref %s" % ref.id)
            # We're back -- the ref should be available now.
            ref_fp = open(runtime_response["filename"], "r")
            obj = pickle.load(ref_fp)
            ref_fp.close()
            ref_cache[ref.id] = obj
            return obj

def deref_json(ref):

    global halt_reason
    global ref_cache
    global halt_spawn_id

    if ref.id in ref_cache:
        return ref_cache[ref.id]
    else:
        for tries in range(2):
            pickle.dump({"request": "deref_json", "id": ref.id}, runtime_out)
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
            def ids_to_refs(x):
                if isinstance(x, str):
                    if x.startswith("__ref__:"):
                        return OpaqueReference(x[8:])
                return x
            obj_with_refs = map_leaf_values(ids_to_refs, obj)
            ref_cache[ref.id] = obj_with_refs
            return obj_with_refs

def spawn(spawn_callable):
    
    new_coro = stackless.coroutine()
    new_coro.bind(spawn_callable)
    save_obj = ResumeState(PersistentState(), new_coro)
    new_coro_fd, new_coro_fname = tempfile.mkstemp()
    new_coro_fp = os.fdopen(new_coro_fd)
    pickle.dump(save_obj, new_coro_fp)
    new_coro_fp.close()
    new_task_id = create_spawned_task_name()
    output_id = create_spawn_output_name(new_task_id)
    pickle.dump({"request": "spawn", "coro_filename": new_coro_fname, "new_task_id": new_task_id, "output_id": output_id}, runtime_out)
    return OpaqueReference(output_id)

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
    elif isinstance(value, OpaqueReference):
        hash.update('ref')
        hash_update_with_structure(hash, value.id)
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
    expected_output_ids = get_names_for_exec(exec_prefix, num_outputs)
    real_args_dict = map_leaf_values(refs_to_strings, exec_args_dict)
    pickle.dump({"request": "spawn_exec",
                 "args": real_args_dict,
                 "new_task_id": new_task_id,
                 "executor_name": exec_name,
                 "exec_prefix": exec_prefix,
                 "output_ids": expected_output_ids}, 
                runtime_out)

    return [OpaqueReference(id) for id in expected_output_ids]

def refs_to_strings(x):
    if isinstance(x, OpaqueReference):
        return x.id
    else:
        return x

def sync_exec(exec_name, exec_args_dict, n_outputs):

    global halt_spawn_id
    global halt_reason

    for tries in range(2):

        real_args_dict = map_leaf_values(refs_to_strings, exec_args_dict)
        pickle.dump({"request": "exec",
                     "executor_name": exec_name,
                     "args": real_args_dict,
                     "n_outputs": n_outputs},
                    runtime_out)
        runtime_out.flush()
        result = pickle.load(runtime_in)
        if result["success"]:
            return [OpaqueReference(x) for x in result["output_names"]]
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

    main_coro.switch()
    try:
        script_return_val = entry_point()
        halt_reason = HALT_DONE
    except Exception, e:
        script_return_val = e
        script_backtrace = traceback.format_exc()
        halt_reason = HALT_RUNTIME_EXCEPTION
        
    main_coro.switch()
    
