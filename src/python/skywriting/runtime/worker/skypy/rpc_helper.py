
from __future__ import with_statement

import os
import select
import sys
import pickle

class RpcRequest:

    def __init__(self):

        self.response = None

class RpcHelper:

    def __init__(self, in_fp, out_fp, active_outputs):

        self.in_fp = in_fp
        self.in_fd = in_fp.fileno()
        self.out_fp = out_fp
        self.active_outputs = active_outputs
        self.pending_request = None

    def drain_receive_buffer(self):

        while True:
            if not self.receive_message(block=False):
                break

    def receive_message(self, block=True):

        if block:
            pargs = []
        else:
            pargs = [0.0]

        reads, _, _ = select.select([self.in_fd], [], [], *pargs)

        have_message = self.in_fd in reads
        if have_message:
            next_message = pickle.load(self.in_fp)
            if next_message["request"] == "subscribe" or next_message["request"] == "unsubscribe":
                self.active_outputs.handle_request(next_message)
            else:
                if self.pending_request is not None:
                    self.pending_request.response = next_message
                else:
                    print >>sys.stderr, "Ignored request", next_message
        return have_message

    def synchronous_request(self, request):
        

        self.pending_request = RpcRequest()
        self.send_message(request)
        while self.pending_request.response is None:
            self.receive_message(block=True)
            ret = self.pending_request.response
        self.pending_request = None
        return ret

    def send_message(self, message):
        
        pickle.dump(message, self.out_fp)
        self.out_fp.flush()

