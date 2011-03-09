
import threading

class FileOutputRecords:

    def __init__(self):

        self.ongoing_outputs = dict()
        self.lock = threading.Lock()

    def set_message_thread(self, thread):
        self.message_thread = thread

    def handle_request(self, message):

        if message["request"] == "subscribe":
            with self.lock:
                try:
                    output = ongoing_outputs[message["id"]]
                    output.subscribe(message["chunk_size"])
                except KeyError:
                    print >>sys.stderr, "Ignored subscribe for non-existent output", message["id"]
        elif message["request"] == "unsubscribe":
            with self.lock:
                try:
                    output = ongoing_outputs[message["id"]]
                    output.unsubscribe()
                except KeyError:
                    print >>sys.stderr, "Ignored unsubscribe for output", message["id"], "not in progress"

    def add_output(self, id, output):

        with self.lock:
            self.ongoing_outputs[id] = output

    def remove_output(self, id):

        with self.lock:
            del self.ongoing_outputs[id]

class OutputFile:
    def __init__(self, message_thread, file_outputs, id):
        self.id = id
        self.bytes_written = 0
        self.chunk_size = None
        self.notify_threshold = None
        self.file_outputs = file_outputs
        self.lock = threading.Lock()
        file_outputs.add_output(self.id, self)

    def set_filename(self, filename):
        self.filename = filename
        self.fp = open(self.filename, "w")

    def __enter__(self):
        return self

    def write(self, str):
        self.fp.write(str)
        self.bytes_written += len(str)
        should_notify = False
        with self.lock:
            if self.notify_threshold is not None and self.bytes_written > self.notify_threshold:
                self.notify_threshold += self.chunk_size
                should_notify = True
        if should_notify:
            message_thread.send_message({"request": "advert", "size": self.bytes_written})

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def subscribe(self, new_chunk_size):
        with self.lock:
            self.chunk_size = new_chunk_size
            self.next_threshold = self.chunk_size * ((self.bytes_written / self.chunk_size) + 1)

    def unsubscribe(self):
        with self.lock:
            self.chunk_size = None
            self.next_threshold = None

    def close(self):
        self.closed = True
        self.fp.close()
        self.file_outputs.remove_output(self.id)
        runtime_response = message_thread.synchronous_request({"request": "close_output", "size": self.bytes_written, "id": self.id})
        self.ref = runtime_response["ref"]

    def rollback(self):
        self.closed = True
        self.fp.close()
        self.file_outputs.remove_output(self.id)
        message_thread.send_message({"request": "rollback_output", "id": self.id})

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is None:
            self.close()
        else:
            self.rollback()
