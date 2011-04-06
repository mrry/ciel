
from __future__ import with_statement

import skypy

def stream_producer(chunk_size, chunks_to_produce, use_direct_pipes):

    bytes_written = 0

    with skypy.open_output(skypy.extra_outputs[0], may_pipe=use_direct_pipes) as file_out:
        while bytes_written < (chunk_size * chunks_to_produce):
            file_out.write("Have some bytes!")
            bytes_written += 16

    return "Wrote %d bytes" % bytes_written

def stream_link(chunk_size, input_ref, use_direct_pipes):

    bytes_written = 0

    # Convoluted structure to avoid blocking on a ref whilst we've got an output in progress
    with skypy.open_output(skypy.extra_outputs[0], may_pipe=use_direct_pipes) as out_file:
        with skypy.deref_as_raw_file(input_ref, may_stream=True, chunk_size=chunk_size) as in_file:
            while True:
                buf = in_file.read(4096)
                if len(buf) == 0:
                    break
                out_file.write(buf)
                bytes_written += len(buf)

    return "Read/wrote %d bytes" % bytes_written

def stream_consumer(chunk_size, in_ref):

    bytes_read = 0

    with skypy.deref_as_raw_file(in_ref, may_stream=True, chunk_size=chunk_size) as in_file:
        while True:
            str = in_file.read(4096)
            bytes_read += len(str)
            if len(str) == 0:
                break

    return "Read %d bytes" % bytes_read

def skypy_main(n_links, n_chunks, use_direct_pipes):

    if use_direct_pipes.find("true") != -1:
        use_direct_pipes = True
    elif use_direct_pipes.find("false") != -1:
        use_direct_pipes = False
    else:
        raise Exception("Bad boolean: use_direct_pipes=%s" % use_direct_pipes)

    n_links = int(n_links)
    n_chunks = int(n_chunks)

    producer = skypy.spawn(stream_producer, 262144, n_chunks, use_direct_pipes, n_extra_outputs=1)

    links_out = []
    for i in range(n_links):
        if i == 0:
            input_ref = producer.extra_outputs[0]
        else:
            input_ref = links_out[-1].extra_outputs[0]
        links_out.append(skypy.spawn(stream_link, 262144, input_ref, use_direct_pipes, extra_dependencies=[input_ref], n_extra_outputs=1))

    consumer_out = skypy.spawn(stream_consumer, 262144, links_out[-1].extra_outputs[0], extra_dependencies=[links_out[-1].extra_outputs[0]])

    ret_outs = [producer.ret_output]
    ret_outs.extend([x.ret_output for x in links_out])
    ret_outs.append(consumer_out)
    with skypy.RequiredRefs(ret_outs):
        results = [skypy.deref(x) for x in ret_outs]

    return "The %d streamers' reports are: %s\n" % (n_links + 2, results)
