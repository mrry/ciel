
from StringIO import StringIO
import os
import tempfile
import simplejson
import struct
from shared.references import json_decode_object_hook, SWReferenceJSONEncoder

class MaybeFile:

    def __init__(self, threshold_bytes=1024, filename=None, open_callback=None):
        self.real_fp = None
        self.filename = filename
        self.str = None
        self.open_callback = open_callback
        self.bytes_written = 0
        self.fake_fp = StringIO()
        self.threshold_bytes = threshold_bytes

    def write(self, str):
        if self.real_fp is not None:
            self.real_fp.write(str)
        else:
            if self.bytes_written + len(str) > self.threshold_bytes:
                if self.open_callback is not None:
                    self.real_fp = self.open_callback()
                elif self.filename is None:
                    self.fd, self.filename = tempfile.mkstemp()
                    self.real_fp = os.fdopen(self.fd, "w")
                else:
                    self.real_fp = open(self.filename, "w")
                self.real_fp.write(self.fake_fp.getvalue())
                self.real_fp.write(str)
                self.fake_fp.close()
                self.fake_fp = None
            else:
                self.fake_fp.write(str)
        self.bytes_written += len(str)

    def __enter__(self):
        return self

    def __exit__(self, extype, exvalue, extraceback):
        if self.real_fp is not None:
            self.real_fp.close()
        if self.fake_fp is not None:
            self.str = self.fake_fp.getvalue()
            self.fake_fp.close()
            
def write_framed_json(obj, fp):
    json_string = simplejson.dumps(obj, cls=SWReferenceJSONEncoder)
    fp.write(struct.pack('!I', len(json_string)))
    fp.write(json_string)
    fp.flush()
    
def read_framed_json(fp):
    request_len, = struct.unpack_from('!I', fp.read(4))
    request_string = fp.read(request_len)
    return simplejson.loads(request_string, object_hook=json_decode_object_hook)

