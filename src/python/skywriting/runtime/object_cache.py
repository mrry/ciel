
import pickle
import simplejson
from cStringIO import StringIO
from shared.references import SWReferenceJSONEncoder, json_decode_object_hook
from skywriting.runtime.executor_helpers import ref_from_string, retrieve_strings_for_refs

def decode_handle(file):
    return file
def encode_noop(obj, file):
    return file.write(obj)
def decode_noop(file):
    return file.read()    
def encode_json(obj, file):
    return simplejson.dump(obj, file, cls=SWReferenceJSONEncoder)
def decode_json(file):
    return simplejson.load(file, object_hook=json_decode_object_hook)
def encode_pickle(obj, file):
    return pickle.dump(obj, file)
def decode_pickle(file):
    return pickle.load(file)

encoders = {'noop': encode_noop, 'json': encode_json, 'pickle': encode_pickle}
decoders = {'noop': decode_noop, 'json': decode_json, 'pickle': decode_pickle, 'handle': decode_handle}

object_cache = {}

def cache_object(object, encoder, id):
    object_cache[(id, encoder)] = object        

def ref_from_object(object, encoder, id):
    """Encodes an object, returning either a DataValue or ConcreteReference as appropriate"""
    cache_object(object, encoder, id)
    buffer = StringIO()
    encoders[encoder](object, buffer)
    ret = ref_from_string(buffer.getvalue(), id)
    buffer.close()
    return ret

def retrieve_objects_for_refs(ref_and_decoders, task_record):

    solutions = dict()
    unsolved_refs = []
    for (ref, decoder) in ref_and_decoders:
        try:
            solutions[ref.id] = object_cache[(ref.id, decoder)]
        except:
            unsolved_refs.append(ref)

    strings = retrieve_strings_for_refs(unsolved_refs, task_record)
    str_of_ref = dict([(ref.id, string) for (string, ref) in zip(strings, unsolved_refs)])
            
    for (ref, decoder) in ref_and_decoders:
        if ref.id not in solutions:
            decoded = decoders[decoder](StringIO(str_of_ref[ref.id]))
            object_cache[(ref.id, decoder)] = decoded
            solutions[ref.id] = decoded
            
    return [solutions[ref.id] for (ref, decoder) in ref_and_decoders]

def retrieve_object_for_ref(ref, decoder, task_record):
    
    return retrieve_objects_for_refs([(ref, decoder)], task_record)[0]
