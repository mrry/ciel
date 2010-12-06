
from StringIO import StringIO
import os
import tempfile

class MaybeFile:

    def __init__(self, threshold_bytes=1024, filename=None):
        self.real_fp = None
        self.filename = filename
        self.bytes_written = 0
        self.fake_fp = StringIO()
        self.threshold_bytes = threshold_bytes

    def write(self, str):
        if self.real_fp is not None:
            self.real_fp.write(str)
        else:
            if self.bytes_written + len(str) > self.threshold_bytes:
                if self.filename is None:
                    self.fd, self.filename = tempfile.mkstemp()
                    self.real_fp = os.fdopen(self.fd, "w")
                else:
                    self.real_fp = open(self.filename, "w")
                self.real_fp.write(self.fake_fp.getvalue())
                self.real_fp.write(str)
                self.fake_fp.close()
                self.fake_fp = None
            else:
                self.bytes_written += len(str)
                self.fake_fp.write(str)

    def __enter__(self):
        return self

    def __exit__(self, extype, exvalue, extraceback):
        if self.real_fp is not None:
            self.real_fp.close()
        if self.fake_fp is not None:
            self.fake_fp.close()

