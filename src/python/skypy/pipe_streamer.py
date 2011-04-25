
from __future__ import with_statement

import skypy

def stream_producer(chunk_size, chunks_to_produce, may_stream, use_direct_pipes):

    bytes_written = 0
    write_string = "A" * 4096

    with skypy.open_output(skypy.get_extra_output_indices()[0], may_stream=may_stream, may_pipe=use_direct_pipes) as file_out:
        while bytes_written < (chunk_size * chunks_to_produce):
            file_out.write(write_string)
            bytes_written += 4096

    return "Wrote %d bytes" % bytes_written

def stream_link(chunk_size, input_ref, may_stream, use_direct_pipes):

    bytes_written = 0

    # Convoluted structure to avoid blocking on a ref whilst we've got an output in progress
    with skypy.open_output(skypy.get_extra_output_indices()[0], may_stream=may_stream, may_pipe=use_direct_pipes) as out_file:
        with skypy.deref_as_raw_file(input_ref, may_stream=may_stream, sole_consumer=use_direct_pipes, chunk_size=chunk_size) as in_file:
            while True:
                buf = in_file.read(4096)
                if len(buf) == 0:
                    break
                out_file.write(buf)
                bytes_written += len(buf)

    return "Read/wrote %d bytes" % bytes_written

def stream_consumer(chunk_size, in_ref, may_stream, use_direct_pipes):

    bytes_read = 0

    with skypy.deref_as_raw_file(in_ref, may_stream=may_stream, sole_consumer=use_direct_pipes, chunk_size=chunk_size) as in_file:
        while True:
            str = in_file.read(4096)
            bytes_read += len(str)
            if len(str) == 0:
                break

    return "Read %d bytes" % bytes_read

def skypy_main(n_links, n_chunks, mode):

    if mode == "sync":
        use_direct_pipes = False
        may_stream = False
    elif mode == "indirect":
        use_direct_pipes = False
        may_stream = True
    elif mode == "direct":
        use_direct_pipes = True
        may_stream = True
    else:
        raise Exception("pipe_streamer.py: bad mode %s" % mode)

    n_links = int(n_links)
    n_chunks = int(n_chunks)

    producer = skypy.spawn(stream_producer, 262144, n_chunks, may_stream, use_direct_pipes, n_extra_outputs=1)

    links_out = []
    for i in range(n_links):
        if i == 0:
            input_ref = producer[1]
        else:
            input_ref = links_out[-1][1]
        links_out.append(skypy.spawn(stream_link, 262144, input_ref, may_stream, use_direct_pipes, extra_dependencies=[input_ref], n_extra_outputs=1))

    if n_links == 0:
        consumer_input = producer[1]
    else:
        consumer_input = links_out[-1][1]
    consumer_out = skypy.spawn(stream_consumer, 262144, consumer_input, may_stream, use_direct_pipes, extra_dependencies=[consumer_input])

    ret_outs = [producer[0]]
    ret_outs.extend([x[0] for x in links_out])
    ret_outs.append(consumer_out)
    
    with skypy.RequiredRefs(ret_outs):
        results = [skypy.deref(x) for x in ret_outs]

    return "The %d streamers' reports are: %s\n" % (n_links + 2, results)
