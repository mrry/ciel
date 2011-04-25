
import sys
from datetime import datetime

cache = dict()

def keys_of_refs(refs):
    refs.sort(key=lambda x: x.id)
    return tuple([ref.id for ref in refs])

def put_cache(refs, tag, obj):
    cache[(keys_of_refs(refs), tag)] = (obj, datetime.now())
    
def try_get_cache(refs, tag):
    cache_key = (keys_of_refs(refs), tag)
    try:
        (obj, insert_time) = cache[cache_key]
    except KeyError:
        return None
    cache[cache_key] = (obj, datetime.now())
    return obj

def get_cache_keys():
    return cache.keys()

def garbage_collect_cache():
    items_in_cache = len(cache)
    items_removed = 0
    now = datetime.now()
    for (key, (obj, insert_time)) in cache.items():
        age = now - insert_time
        if age.seconds > 30:
            items_removed += 1
            del cache[key]
    print >>sys.stderr, "SkyPy: Scanned soft cache, deleted %d/%d old items" % (items_removed, items_in_cache)
