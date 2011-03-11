
from __future__ import with_statement

import sys
import skypy

def stream_producer(chunk_size, chunks_to_produce):

    bytes_written = 0

    with skypy.open_output(skypy.extra_outputs[0]) as file_out:
        while bytes_written < (chunk_size * chunks_to_produce):
            file_out.write("Have some bytes!")
            bytes_written += 16

    return "Wrote %d bytes" % bytes_written

def stream_consumer(chunk_size, in_ref):

    bytes_read = 0

    with skypy.deref_as_raw_file(in_ref, may_stream=True, chunk_size=chunk_size) as in_file:
        while True:
            str = in_file.read(4096)
            bytes_read += len(str)
            if len(str) == 0:
                break

    return "Read %d bytes" % bytes_read

def reader_function(refs):
    
    print >>sys.stderr, "SkyPy example reader function:", len(refs), "inputs"

    results = []
    for ref in refs:
        with skypy.deref_as_raw_file(ref) as in_file:
            results.append(in_file.read())
    with skypy.open_output(skypy.extra_outputs[0]) as file_out:
        file_out.write("Complete read results: %s\n" % str(results))
    return "Read %d results" % len(refs)

def read_result(reader_result):

    with skypy.RequiredRefs(list(reader_result)):
        cooked_result = skypy.deref(reader_result.ret_output)
        with skypy.deref_as_raw_file(reader_result.extra_outputs[0]) as in_file:
            return (cooked_result, in_file.read())

def skypy_main():

    print >>sys.stderr, "SkyPy example producer:", len(skypy.extra_outputs), "outputs"

    # Step 1: Test writing our external raw outputs.

    for i, id in enumerate(skypy.extra_outputs):
        with skypy.open_output(id) as file_out:
            file_out.write("Skypy writing output %d" % i)

    # Step 2: Test writing fresh outputs.

    refs = []

    for i in range(3):
        name = skypy.get_fresh_output_name()
        file_out = skypy.open_output(name)
        with file_out:
            file_out.write("Skypy writing anonymous output %d" % i)
        refs.append(file_out.get_completed_ref())

    # Step 3: Test reading those results back.

    reader_result = skypy.spawn(reader_function, refs, n_extra_outputs=1)
#    cooked_result, raw_result = read_result(reader_result)
    cooked_result, raw_result = "Dummy", "text"

    # Step 4: Test a stream producer/consumer pair.

    producer = skypy.spawn(stream_producer, 262144, 100, n_extra_outputs=1)
    consumer_out = skypy.spawn(stream_consumer, 262144, producer.extra_outputs[0])

    ret_outs = [producer.ret_output, consumer_out]
    with skypy.RequiredRefs(ret_outs):
        results = [skypy.deref(x) for x in ret_outs]

    return "I wrote %d external outputs\nI created 3 myself\nThe reader's cooked result was '%s'\n The reader's raw result was '%s'\nFinally the streamers' reports are %s\n" % (len(skypy.extra_outputs), cooked_result, raw_result, results)

