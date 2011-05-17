
import skypy
import os
import select
import fcntl

from errno import EAGAIN

class CompleteFile:

    def __init__(self, ref, filename, chunk_size=None, must_close=False):
        self.ref = ref
        self.filename = filename
        self.chunk_size = chunk_size
        self.must_close = must_close
        self.fp = open(self.filename, "r")
        skypy.add_ref_dependency(self.ref)

    def close(self):
        self.fp.close()
        if self.must_close:
            skypy.current_task.message_helper.send_message("close_stream", {"id": self.ref.id, "chunk_size": self.chunk_size})            
        skypy.remove_ref_dependency(self.ref)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        self.close()

    def __getattr__(self, name):
        return getattr(self.fp, name)

    def __getstate__(self):
        if self.fp.closed:
            return (self.ref, None, None, None)
        else:
            return (self.ref, self.fp.tell(), self.chunk_size, self.must_close)

    def __setstate__(self, (ref, offset, chunk_size, must_close)):
        self.ref = ref
        if offset is not None:
            if must_close is True:
                runtime_response = skypy.fetch_ref(self.ref, "open_ref_async", chunk_size=chunk_size)
                self.must_close = runtime_response["blocking"] and not runtime_response["done"]
                self.chunk_size = chunk_size
            else:
                runtime_response = skypy.fetch_ref(self.ref, "open_ref")
            self.filename = runtime_response["filename"]
            self.fp = open(self.filename, "r")
            self.fp.seek(offset, os.SEEK_SET)
        # Else this is a closed file object.

class StreamingFile:
    
    def __init__(self, ref, filename, initial_size, chunk_size):
        self.ref = ref
        self.filename = filename
        self.chunk_size = chunk_size
        self.really_eof = False
        self.current_size = None
        self.fp = open(self.filename, "r")
        self.closed = False
        self.softspace = False
        skypy.add_ref_dependency(self.ref)

    def __enter__(self):
        return self

    def close(self):
        self.closed = True
        self.fp.close()
        skypy.current_task.message_helper.send_message("close_stream", {"id": self.ref.id, "chunk_size": self.chunk_size})
        skypy.remove_ref_dependency(self.ref)

    def __exit__(self, exnt, exnv, exnbt):
        self.close()

    def wait(self, **kwargs):
        out_dict = {"id": self.ref.id}
        out_dict.update(kwargs)
        runtime_response = skypy.current_task.message_helper.synchronous_request("wait_stream", out_dict)
        if not runtime_response["success"]:
            raise Exception("File transfer failed before EOF")
        else:
            self.really_eof = runtime_response["done"]
            self.current_size = runtime_response["size"]

    def wait_bytes(self, bytes):
        bytes = self.chunk_size * ((bytes / self.chunk_size) + 1)
        self.wait(bytes=bytes)

    def read(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.read(*pargs)
            if self.really_eof or (bytes is not None and len(ret) == bytes):
                return ret
            else:
                self.fp.seek(-len(ret), os.SEEK_CUR)
                if bytes is None:
                    self.wait(eof=True)
                else:
                    self.wait_bytes(self.fp.tell() + bytes)

    def readline(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.readline(*pargs)
            if self.really_eof or (bytes is not None and len(ret) == bytes) or ret[-1] == "\n":
                return ret
            else:
                self.fp.seek(-len(ret), os.SEEK_CUR)
                # I wait this long whether or not the byte-limit is set in the hopes of finding a \n before then.
                self.wait_bytes(self.fp.tell() + len(ret) + 128)

    def readlines(self, *pargs):
        if len(pargs) > 0:
            bytes = pargs[0]
        else:
            bytes = None
        while True:
            ret = self.fp.readlines(*pargs)
            bytes_read = 0
            for line in ret:
                bytes_read += len(line)
            if self.really_eof or (bytes is not None and bytes_read == bytes) or ret[-1][-1] == "\n":
                return ret
            else:
                self.fp.seek(-bytes_read, os.SEEK_CUR)
                self.wait_bytes(self.fp.tell() + bytes_read + 128)

    def xreadlines(self):
        return self

    def __iter__(self):
        return self

    def next(self):
        ret = self.readline()
        if ret == "\n":
            raise StopIteration()

    def __getstate__(self):
        if not self.fp.closed:
            return (self.ref, self.fp.tell(), self.chunk_size)
        else:
            return (self.ref, None, self.chunk_size)

    def __setstate__(self, (ref, offset, chunk_size)):
        self.ref = ref
        self.chunk_size = chunk_size
        if offset is not None:
            runtime_response = skypy.fetch_ref(self.ref, "open_ref_async", chunk_size=chunk_size)
            self.really_eof = runtime_response["done"]
            self.current_size = runtime_response["size"]
            self.fp = open(runtime_response["filename"], "r")
            self.fp.seek(offset, os.SEEK_SET)
        # Else we're already closed
