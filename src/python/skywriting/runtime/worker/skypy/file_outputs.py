
from __future__ import with_statement

import sys

class FileOutputRecords:

    def __init__(self):

        self.ongoing_outputs = dict()

    def set_message_helper(self, helper):
        self.message_helper = helper

    def handle_request(self, message):

        if message["request"] == "subscribe":
            try:
                output = self.ongoing_outputs[message["id"]]
                output.subscribe(message["chunk_size"])
            except KeyError:
                print >>sys.stderr, "Ignored subscribe for non-existent output", message["id"]
        elif message["request"] == "unsubscribe":
            try:
                output = self.ongoing_outputs[message["id"]]
                output.unsubscribe()
            except KeyError:
                print >>sys.stderr, "Ignored unsubscribe for output", message["id"], "not in progress"

    def add_output(self, id, output):
        self.ongoing_outputs[id] = output

    def remove_output(self, id):
        del self.ongoing_outputs[id]

class OutputFile:
    def __init__(self, message_helper, file_outputs, id):
        self.id = id
        self.bytes_written = 0
        self.chunk_size = None
        self.notify_threshold = None
        self.ref = None
        self.message_helper = message_helper
        self.file_outputs = file_outputs
        self.closed = False
        file_outputs.add_output(self.id, self)

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
            self.message_helper.send_message({"request": "advert", "id": self.id, "size": self.bytes_written})

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
        self.file_outputs.remove_output(self.id)
        runtime_response = self.message_helper.synchronous_request({"request": "close_output", "size": self.bytes_written, "id": self.id})
        self.ref = runtime_response["ref"]

    def get_completed_ref(self):
        if self.ref is None:
            raise Exception("Tried to get completed ref for %s before it was closed" % self.id)
        return self.ref

    def rollback(self):
        self.closed = True
        self.fp.close()
        self.file_outputs.remove_output(self.id)
        self.message_helper.send_message({"request": "rollback_output", "id": self.id})

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is None:
            self.close()
        else:
            self.rollback()

    def __getstate__(self):
        print >>sys.stderr, "GETSTATE CALLED", self.id
        if self.closed:
            return (self.id, self.ref)
        else:
            raise Exception("Can't pickle open output-file with id '%s'" % self.id)

    def __setstate__(self, (id, ref)):
        self.id = id
        self.ref = ref
        self.closed = True
