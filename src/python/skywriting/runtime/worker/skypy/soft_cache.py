
cache = dict()

def keys_of_refs(refs):
    refs.sort(key=lambda x: x.id)
    return tuple([ref.id for ref in refs])

def put_cache(refs, tag, obj):
    cache[(keys_of_refs(refs), tag)] = obj
    
def try_get_cache(refs, tag):
    try:
        return cache[(keys_of_refs(refs), tag)]
    except KeyError:
        return None

def get_cache_keys():
    return cache.keys()