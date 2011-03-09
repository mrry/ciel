
from __future__ import with_statement

import threading
import os
import select
import sys
import pickle

class RpcRequest:

    def __init__(self):

        self.response = None
        self.event = threading.Event()

class RpcThread:

    def __init__(self, in_fp, out_fp, active_outputs):

        self.in_fp = in_fp
        self.in_fd = in_fp.fileno()
        self.out_fp = out_fp
        self.active_outputs = active_outputs
        self.lock = threading.Lock()
        self.synchronous_request = None

    def start(self):
        self.death_pipe_read, self.death_pipe_write = os.pipe()
        self.thread = threading.Thread(self.thread_main)
        self.thread.start()

    def thread_main(self):

        while True:
            receive_message()

    def receive_message(self):
        
        reads, _, _ = select.select([self.in_fd, self.death_pipe_read])

        if self.death_pipe_read in reads:
            print >>sys.stderr, "Event loop ended"

        if self.in_fd in reads:
            next_message = pickle.load(self.in_fp)
            if next_message["request"] == "subscribe" or next_message["request"] == "unsubscribe":
                self.active_outputs.handle_request(next_message)
            else:
                if self.synchronous_request is not None:
                    self.synchronous_request.response = next_message
                    self.synchronous_request.event.set()
                else:
                    print >>sys.stderr, "Ignored request", next_message

    def synchronous_request(self, request):
        
        with self.lock:
            self.synchronous_request = RpcRequest()
            pickle.dump(request, self.out_fp)
        self.out_fp.flush()
        self.synchronous_request.wait()
        ret = self.synchronous_request.response
        self.synchronous_request = None
        return ret

    def send_message(self, message):
        
        with self.lock:
            pickle.dump(message, self.out_fp)
        self.out_fp.flush()

    def kill_thread(self):

        os.write(self.death_pipe_write, "X")
        self.thread.join()
