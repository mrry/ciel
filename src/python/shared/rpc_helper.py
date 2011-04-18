
from __future__ import with_statement

import select
import sys
from shared.io_helpers import read_framed_json, write_framed_json

class ShutdownException(Exception):
    
    def __init__(self, reason):
        self.reason = reason

class RpcRequest:

    def __init__(self, method):

        self.response = None
        self.method = method

class RpcHelper:

    def __init__(self, in_fp, out_fp, active_outputs=None):

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
            (method, args) = read_framed_json(self.in_fp)
            if method == "subscribe" or method == "unsubscribe":
                if self.active_outputs is None:
                    print >>sys.stderr, "Ignored request", method, "args", args, "because I have no active outputs dict"
                else:
                    self.active_outputs.handle_request(method, args)
            elif method == "die":
                raise ShutdownException(args["reason"])
            else:
                if self.pending_request is not None:
                    if method != self.pending_request.method:
                        print >>sys.stderr, "Ignored response of type", method, \
                            "because I'm waiting for", self.pending_request.method
                    self.pending_request.response = args
                else:
                    print >>sys.stderr, "Ignored request", method, "args", args
        return have_message

    def synchronous_request(self, method, args=None, send=True):
        
        self.pending_request = RpcRequest(method)
        if send:
            self.send_message(method, args)
        while self.pending_request.response is None:
            self.receive_message(block=True)
            ret = self.pending_request.response
        self.pending_request = None
        return ret
    
    def await_message(self, method):
        return self.synchronous_request(method, send=False)

    def send_message(self, method, args):
        
        write_framed_json((method, args), self.out_fp)
        self.out_fp.flush()

