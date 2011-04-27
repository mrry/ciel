
from __future__ import with_statement

import sys

class FileOutputRecords:

    def __init__(self):

        self.ongoing_outputs = dict()

    def set_message_helper(self, helper):
        self.message_helper = helper

    def handle_request(self, method, args):

        if method == "subscribe":
            try:
                output = self.ongoing_outputs[args["index"]]
                output.subscribe(args["chunk_size"])
            except KeyError:
                print >>sys.stderr, "Ignored subscribe for non-existent output", args["index"]
        elif method == "unsubscribe":
            try:
                output = self.ongoing_outputs[args["index"]]
                output.unsubscribe()
            except KeyError:
                print >>sys.stderr, "Ignored unsubscribe for output", args["index"], "not in progress"

    def add_output(self, index, output):
        self.ongoing_outputs[index] = output

    def remove_output(self, index):
        del self.ongoing_outputs[index]

class OutputFile:
    def __init__(self, message_helper, file_outputs, index):
        self.index = index
        self.bytes_written = 0
        self.chunk_size = None
        self.notify_threshold = None
        self.ref = None
        self.message_helper = message_helper
        self.file_outputs = file_outputs
        self.closed = False
        file_outputs.add_output(self.index, self)

    def set_filename(self, filename):
        self.filename = filename
        self.fp = open(self.filename, "w")

    def __enter__(self):
        return self

    def write(self, str):
        self.message_helper.drain_receive_buffer()
        self.fp.write(str)
        self.bytes_written += len(str)
        should_notify = False
        if self.notify_threshold is not None and self.bytes_written > self.notify_threshold:
            self.notify_threshold += self.chunk_size
            should_notify = True
        if should_notify:
            self.message_helper.send_message("advert", {"index": self.index, "size": self.bytes_written})

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def subscribe(self, new_chunk_size):
        self.chunk_size = new_chunk_size
        self.notify_threshold = self.chunk_size * ((self.bytes_written / self.chunk_size) + 1)

    def unsubscribe(self):
        self.chunk_size = None
        self.notify_threshold = None

    def close(self):
        self.closed = True
        self.fp.close()
        self.file_outputs.remove_output(self.index)
        runtime_response = self.message_helper.synchronous_request("close_output", {"size": self.bytes_written, "index": self.index})
        self.ref = runtime_response["ref"]

    def get_completed_ref(self):
        if self.ref is None:
            raise Exception("Tried to get completed ref for output %s before it was closed" % self.index)
        return self.ref

    def rollback(self):
        self.closed = True
        self.fp.close()
        self.file_outputs.remove_output(self.index)
        self.message_helper.send_message("rollback_output", {"index": self.index})

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is None:
            self.close()
        else:
            self.rollback()

    def __getstate__(self):
        if self.closed:
            return (self.index, self.ref)
        else:
            raise Exception("Can't pickle open output-file with index '%s'" % self.index)

    def __setstate__(self, (index, ref)):
        self.index = index
        self.ref = ref
        self.closed = True
