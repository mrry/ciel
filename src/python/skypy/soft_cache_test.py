
from __future__ import with_statement

import skypy
import soft_cache

def counter(ref):
    
    cached_result = soft_cache.try_get_cache([ref], "wc")
    if cached_result is not None:
        print "Counter: got cached result! It was", cached_result
    else:
        with skypy.deref_as_raw_file(ref, make_sweetheart=True) as in_fp:
            cached_result = len(in_fp.read())
        print "Counter: calculated result anew. It was", cached_result
        soft_cache.put_cache([ref], "wc", cached_result)
    return cached_result            

def skypy_main():
    
    new_output_index = skypy.get_fresh_output_index()
    out_fp = skypy.open_output(new_output_index)
    with out_fp:
        for i in range(100000):
            out_fp.write("Whoozit")
    out_ref = out_fp.get_completed_ref()
    first_counter = skypy.spawn(counter, out_ref, extra_dependencies=[out_ref])
    second_counter = skypy.spawn(counter, out_ref, extra_dependencies=[out_ref])
    with skypy.RequiredRefs([first_counter, second_counter]):
        first_result = skypy.deref(first_counter)
        second_result = skypy.deref(second_counter)
    
    return "First counter said", first_result, "and second one said", second_result
    
    
    