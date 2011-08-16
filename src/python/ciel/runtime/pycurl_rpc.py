# Copyright (c) 2010--11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

from cStringIO import StringIO
import threading
import ciel.runtime.pycurl_thread
import pycurl

class pycURLBufferContext(ciel.runtime.pycurl_thread.pycURLContext):

    def __init__(self, method, in_str, out_fp, url, result_callback):
        
        ciel.runtime.pycurl_thread.pycURLContext.__init__(self, url, result_callback)

        self.write_fp = out_fp

        self.curl_ctx.setopt(pycurl.WRITEFUNCTION, self.write)
        if method == "POST":
            self.curl_ctx.setopt(pycurl.POST, True)
            self.curl_ctx.setopt(pycurl.POSTFIELDS, in_str)
            self.curl_ctx.setopt(pycurl.POSTFIELDSIZE, len(in_str))
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Content-Type: application/octet-stream", "Expect:"])

    def write(self, data):
        self.write_fp.write(data)
        return len(data)

class BufferTransferContext:
        
    def __init__(self, method, url, postdata, result_callback=None):
        
        self.response_buffer = StringIO()
        self.completed_event = threading.Event()
        self.result_callback = result_callback
        self.url = url
        self.curl_ctx = pycURLBufferContext(method, postdata, self.response_buffer, url, self.result)

    def start(self):

        self.curl_ctx.start()

    def get_result(self):

        self.completed_event.wait()
        if self.success:
            return self.response_string
        else:
            raise Exception("Curl-post failed. Possible error-document: %s" % self.response_string)

    def result(self, success):
            
        self.response_string = self.response_buffer.getvalue()
        self.success = success
        self.response_buffer.close()
        self.completed_event.set()
        if self.result_callback is not None:
            self.result_callback(success, self.url)

# Called from cURL thread
def _post_string_noreturn(url, postdata, result_callback=None):
    ctx = BufferTransferContext("POST", url, postdata, result_callback)
    ctx.start()

def post_string_noreturn(url, postdata, result_callback=None):
    ciel.runtime.pycurl_thread.do_from_curl_thread(lambda: _post_string_noreturn(url, postdata, result_callback))

# Called from cURL thread
def _post_string(url, postdata):
    ctx = BufferTransferContext("POST", url, postdata)
    ctx.start()
    return ctx

def post_string(url, postdata):
    ctx = ciel.runtime.pycurl_thread.do_from_curl_thread_sync(lambda: _post_string(url, postdata))
    return ctx.get_result()

# Called from the cURL thread
def _get_string(url):
    ctx = BufferTransferContext("GET", url, "")
    ctx.start()
    return ctx

def get_string(url):
    ctx = ciel.runtime.pycurl_thread.do_from_curl_thread_sync(lambda: _get_string(url))
    return ctx.get_result()
