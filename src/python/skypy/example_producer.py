
from __future__ import with_statement

import sys
import skypy

def reader_function(refs):
    
    print >>sys.stderr, "SkyPy example reader function:", len(refs), "inputs"

    results = []
    for ref in refs:
        with skypy.deref_as_raw_file(ref) as in_file:
            results.append(in_file.read())
    with skypy.open_output(skypy.extra_outputs[0]) as file_out:
        file_out.write("Complete read results: %s\n" % str(results))
    return "Read %d results" % len(refs)

def skypy_main():

    print >>sys.stderr, "SkyPy example producer:", len(skypy.extra_outputs), "outputs"

    for i, id in enumerate(skypy.extra_outputs):
        with skypy.open_output(id) as file_out:
            file_out.write("Skypy writing output %d" % i)

    refs = []

    for i in range(3):
        name = skypy.get_fresh_output_name()
        file_out = skypy.open_output(name)
        with file_out:
            file_out.write("Skypy writing anonymous output %d" % i)
        refs.append(file_out.get_completed_ref())

    reader_result = skypy.spawn(reader_function, refs, n_extra_outputs=1)

    with skypy.RequiredRefs(list(reader_result)):
        cooked_result = skypy.deref(reader_result.ret_output)
        with skypy.deref_as_raw_file(reader_result.extra_outputs[0]) as in_file:
            raw_result = in_file.read()

    return "I wrote %d external outputs\nI created 3 myself\nThe reader's cooked result was '%s'\n The reader's raw result was '%s'\n" % (len(skypy.extra_outputs), cooked_result, raw_result)

