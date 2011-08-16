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

import ciel
import logging
import simplejson
import threading
import os

from ciel.runtime.pycurl_rpc import post_string_noreturn
from ciel.runtime.block_store import filename
from ciel.runtime.producer import get_producer_for_id

# Remote endpoints that are receiving adverts from our streaming producers.
# Indexed by (refid, otherend_netloc)
remote_stream_subscribers = dict()

module_lock = threading.Lock()

class RemoteOutputSubscriber:
        
    def __init__(self, file_output, netloc, chunk_size):
        self.file_output = file_output
        self.netloc = netloc
        self.chunk_size = chunk_size
        self.current_size = None
        self.last_notify = None

    def set_chunk_size(self, chunk_size):
        self.chunk_size = chunk_size
        if self.current_size is not None:
            self.post(simplejson.dumps({"bytes": self.current_size, "done": False}))
        self.file_output.chunk_size_changed(self)

    def unsubscribe(self):
        self.file_output.unsubscribe(self)

    def post(self, message):
        post_string_noreturn("http://%s/control/streamstat/%s/advert" % (self.netloc, self.file_output.refid), message)

    def progress(self, bytes):
        self.current_size = bytes
        if self.last_notify is None or self.current_size - self.last_notify > self.chunk_size:
            data = simplejson.dumps({"bytes": bytes, "done": False})
            self.post(data)
            self.last_notify = self.current_size

    def result(self, success):
        if success:
            self.post(simplejson.dumps({"bytes": self.current_size, "done": True}))
        else:
            self.post(simplejson.dumps({"failed": True}))

# Remote is subscribing to updates regarding an output. Be helpful and inform him of a completed file, too.
def subscribe_output(otherend_netloc, chunk_size, id):
    post = None
    with module_lock:
        try:
            producer = get_producer_for_id(id)
            if producer is not None:
                try:
                    remote_stream_subscribers[(id, otherend_netloc)].set_chunk_size(chunk_size)
                    ciel.log("Remote %s changed chunk size for %s to %d" % (otherend_netloc, id, chunk_size), "BLOCKSTORE", logging.DEBUG)
                except KeyError:
                    new_subscriber = RemoteOutputSubscriber(producer, otherend_netloc, chunk_size)
                    producer.subscribe(new_subscriber, try_direct=False)
                    ciel.log("Remote %s subscribed to output %s (chunk size %d)" % (otherend_netloc, id, chunk_size), "BLOCKSTORE", logging.DEBUG)
            else:
                try:
                    st = os.stat(filename(id))
                    post = simplejson.dumps({"bytes": st.st_size, "done": True})
                except OSError:
                    post = simplejson.dumps({"absent": True})
        except Exception as e:
            ciel.log("Subscription to %s failed with exception %s; reporting absent" % (id, e), "BLOCKSTORE", logging.WARNING)
            post = simplejson.dumps({"absent": True})
    if post is not None:
        post_string_noreturn("http://%s/control/streamstat/%s/advert" % (otherend_netloc, id), post)

def unsubscribe_output(otherend_netloc, id):
    with module_lock:
        try:
            remote_stream_subscribers[(id, otherend_netloc)].cancel()
            ciel.log("%s unsubscribed from %s" % (otherend_netloc, id), "BLOCKSTORE", logging.DEBUG)
        except KeyError:
            ciel.log("Ignored unsubscribe request for unknown block %s" % id, "BLOCKSTORE", logging.WARNING)
