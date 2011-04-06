
from __future__ import with_statement

import skypy
import sys

def skypy_main(run_seconds, async, direct):
    
    if async.find("true") != -1:
        may_stream = True
    else:
        may_stream = False
    if direct.find("true") != -1:
        try_direct = True
    else:
        try_direct = False
    tests_jar = skypy.package_lookup("java_tests")
    refs = skypy.spawn_exec("java", args={"inputs": [], "argv": [str(run_seconds)], "lib": [tests_jar], "class": "tests.JitteryProducer", "stream_output": True, "pipe_output": try_direct}, n_outputs=2)

    got_bytes = 0

    with skypy.deref_as_raw_file(refs[0], may_stream=may_stream, sole_consumer=try_direct, chunk_size=1048576) as file_in:
        while True:
            file_str = file_in.read(1048576)
            if len(file_str) == 0:
                break
            print >>sys.stderr, "Read", len(file_str), "bytes"
            got_bytes += len(file_str)
    with skypy.deref_as_raw_file(refs[1]) as n_bytes:
        byte_count = n_bytes.read()

    return "Producer wrote %s, I got %d starting with %s" % (byte_count, got_bytes, file_str[:20])
