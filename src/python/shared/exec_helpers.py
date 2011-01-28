
import hashlib
from shared.references import SWRealReference

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

def get_exec_prefix(executor_name, real_args, num_outputs):
    sha = hashlib.sha1()
    hash_update_with_structure(sha, [real_args, num_outputs])
    return '%s:%s:' % (executor_name, sha.hexdigest())

def get_exec_output_ids(exec_prefix, num_outputs):
    return ['%s%d' % (exec_prefix, i) for i in range(num_outputs)]

