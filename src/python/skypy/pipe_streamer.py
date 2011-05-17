
from __future__ import with_statement
from datetime import datetime 
import pickle

import skypy

def stream_producer(chunk_size, chunks_to_produce, may_stream, use_direct_pipes):

    chunks_written = 0
    write_string = "A" * 4096
    events = []
    
    events.append(("STARTED", datetime.now()))
    with skypy.open_output(skypy.get_extra_output_indices()[0], may_stream=may_stream, may_pipe=use_direct_pipes) as file_out:
        events.append(("START_WRITE", datetime.now()))
        while chunks_written < chunks_to_produce:
            bytes_written = 0
            while bytes_written < chunk_size:
                file_out.write(write_string)
                bytes_written += 4096
            chunks_written += 1
            events.append(("WROTE_CHUNK", datetime.now()))

    events.append(("FINISHED", datetime.now()))
    
    with skypy.open_output(skypy.get_extra_output_indices()[1]) as log_out:
        pickle.dump(events, log_out)

    return "Wrote %d bytes" % (chunk_size * chunks_to_produce)

def stream_link(chunk_size, input_ref, may_stream, producer_pipe, consumer_pipe, must_block):

    bytes_written = 0

    # Convoluted structure to avoid blocking on a ref whilst we've got an output in progress
    with skypy.open_output(skypy.get_extra_output_indices()[0], may_stream=may_stream, may_pipe=producer_pipe) as out_file:
        with skypy.deref_as_raw_file(input_ref, may_stream=may_stream, sole_consumer=consumer_pipe, chunk_size=chunk_size, must_block=must_block) as in_file:
            while True:
                buf = in_file.read(4096)
                if len(buf) == 0:
                    break
                out_file.write(buf)
                bytes_written += len(buf)

    return "Read/wrote %d bytes" % bytes_written

def stream_consumer(chunk_size, in_ref, may_stream, use_direct_pipes, must_block):

    bytes_read = 0
    next_threshold = chunk_size
    
    events = []
    events.append(("STARTED", datetime.now()))

    with skypy.deref_as_raw_file(in_ref, may_stream=may_stream, sole_consumer=use_direct_pipes, chunk_size=chunk_size, must_block=must_block) as in_file:

        events.append(("START_READ", datetime.now()))
    
        while True:
            str = in_file.read(4096)
            bytes_read += len(str)
            if len(str) == 0:
                break
            if bytes_read >= next_threshold:
                next_threshold += chunk_size
                events.append(("READ_CHUNK", datetime.now()))
        
    events.append(("FINISHED", datetime.now()))
    
    with skypy.open_output(skypy.get_extra_output_indices()[0]) as log_out:
        pickle.dump(events, log_out)

    return "Read %d bytes" % bytes_read

def skypy_main(n_links, n_chunks, mode):

    if mode == "sync":
        producer_pipe = False
        consumer_pipe = False
        consumer_must_block = False
        producer_may_stream = False
        consumer_may_stream = False
    elif mode == "indirect_single_fetch":
        producer_pipe = False
        consumer_pipe = False
        consumer_must_block = False
        producer_may_stream = False
        consumer_may_stream = True
    elif mode == "indirect_pipe":
        producer_pipe = False
        consumer_pipe = False
        consumer_must_block = True
        producer_may_stream = True
        consumer_may_stream = True
    elif mode == "indirect":
        producer_pipe = False
        consumer_pipe = False
        consumer_must_block = False
        producer_may_stream = True
        consumer_may_stream = True
    elif mode == "indirect_tcp":
        producer_pipe = False
        consumer_pipe = True
        consumer_must_block = False
        producer_may_stream = True
        consumer_may_stream = True
    elif mode == "direct":
        producer_pipe = True
        consumer_pipe = True
        consumer_must_block = False
        producer_may_stream = True
        consumer_may_stream = True
    else:
        raise Exception("pipe_streamer.py: bad mode %s" % mode)
    
    n_links = int(n_links)
    n_chunks = int(n_chunks)

    producer = skypy.spawn(stream_producer, 67108864, n_chunks, producer_may_stream, producer_pipe, n_extra_outputs=2)

    links_out = []
    for i in range(n_links):
        if i == 0:
            input_ref = producer[1]
        else:
            input_ref = links_out[-1][1]
        links_out.append(skypy.spawn(stream_link, 67108864, input_ref, producer_may_stream, producer_pipe, consumer_pipe, consumer_must_block, extra_dependencies=[input_ref], n_extra_outputs=1))

    if n_links == 0:
        consumer_input = producer[1]
    else:
        consumer_input = links_out[-1][1]
    if producer_may_stream:
        extra_dependencies = [consumer_input]
    else:
        extra_dependencies = []
    run_fixed = not producer_may_stream
    consumer_out = skypy.spawn(stream_consumer, 67108864, consumer_input, consumer_may_stream, consumer_pipe, consumer_must_block, n_extra_outputs=1, extra_dependencies=extra_dependencies, run_fixed=run_fixed)
    ret_outs = [producer[0]]
    ret_outs.extend([x[0] for x in links_out])
    ret_outs.append(consumer_out[0])
    
    with skypy.RequiredRefs(ret_outs):
        results = [skypy.deref(x) for x in ret_outs]

    return ["The %d streamers' reports are: %s\n" % (n_links + 2, results), producer[2], consumer_out[1]]
