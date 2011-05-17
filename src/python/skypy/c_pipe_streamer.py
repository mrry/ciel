
from __future__ import with_statement
from datetime import datetime 
import pickle

import skypy

def stream_consumer(chunk_size, in_ref, may_stream, use_direct_pipes, must_block, do_log):

    bytes_read = 0
    next_threshold = chunk_size
    
    events = []
    events.append(("STARTED", datetime.now()))

    with skypy.deref_as_raw_file(in_ref, may_stream=may_stream, sole_consumer=use_direct_pipes, chunk_size=chunk_size, must_block=must_block, debug_log=do_log) as in_file:

        events.append(("START_READ", datetime.now()))
    
        while True:
            str = in_file.read(4096)
            bytes_read += len(str)
            if len(str) == 0:
                break
            if bytes_read >= next_threshold:
                next_threshold += chunk_size
                events.append(("READ_CHUNK", datetime.now()))
        try:
            events.extend(in_file.debug_log)
        except:
            pass
        
    events.append(("FINISHED", datetime.now()))
    
    with skypy.open_output(skypy.get_extra_output_indices()[0]) as log_out:
        pickle.dump(events, log_out)

    return "Read %d bytes" % bytes_read

def skypy_main(n_chunks, mode, do_log):

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
    
    if do_log == "true":
        do_log = True
    elif do_log == "false":
        do_log = False
    else:
        raise Exception("pipe_streamer.py: Argument 4 must be boolean (got %s)" % do_log)

    n_chunks = int(n_chunks)

    producer_path = "/opt/smowton-skywriting/src/c/tests/stream_producer"
    consumer_path = "/opt/smowton-skywriting/src/c/tests/stream_consumer"

    #producer_path = "/local/scratch/cs448/skywriting/src/c/tests/stream_producer"
    #consumer_path = "/local/scratch/cs448/skywriting/src/c/tests/stream_consumer"
    
    producer = skypy.spawn_exec("proc", command=producer_path, force_n_outputs=2, proc_pargs=[n_chunks, producer_may_stream, producer_pipe])

    consumer_input = producer[1]

    consumer_out = skypy.spawn_exec("proc", command=consumer_path, force_n_outputs=1, proc_pargs=[consumer_input, consumer_may_stream, consumer_pipe, consumer_must_block])
    
    ret_outs = [consumer_out, producer[0]]
    
    with skypy.RequiredRefs(ret_outs):
    
        with skypy.deref_as_raw_file(consumer_out) as fp:
            consumer_report = fp.read()
        with skypy.deref_as_raw_file(producer[0]) as fp:
            producer_report = fp.read()

    return "Producer reports: %s, Consumer reports: %s" % (producer_report, consumer_report)
