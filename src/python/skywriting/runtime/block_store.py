# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#                    Christopher Smowton <chris.smowton@cl.cam.ac.uk>
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
from __future__ import with_statement
from threading import Lock
from skywriting.runtime.exceptions import \
    MissingInputException, RuntimeSkywritingError
import random
import urllib2
import httplib2
import shutil
import pickle
import os
import uuid
import struct
import tempfile
import logging
import pycurl
import select
import fcntl
import re
import threading
import codecs
from datetime import datetime, timedelta
import time
from cStringIO import StringIO
from errno import EAGAIN, EPIPE
from cherrypy.process import plugins
from shared.io_helpers import MaybeFile

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from shared.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SW2_StreamReference,\
    SW2_TombstoneReference, SW2_FetchReference
from skywriting.runtime.references import SWReferenceJSONEncoder
import hashlib
import contextlib
from skywriting.lang.parser import CloudScriptParser
import skywriting
import ciel
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

PIN_PREFIX = '.__pin__:'

length_regex = re.compile("^Content-Length:\s*([0-9]+)")
http_response_regex = re.compile("^HTTP/1.1 ([0-9]+)")

class StreamRetry:
    pass
STREAM_RETRY = StreamRetry()

def get_netloc_for_sw_url(url):
    return urlparse.urlparse(url).netloc

def get_id_for_sw_url(url):
    return urlparse.urlparse(url).path

def json_decode_object_hook(dict_):
        if '__ref__' in dict_:
            return build_reference_from_tuple(dict_['__ref__'])
        else:
            return dict_

def sw_to_external_url(url):
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme == 'swbs':
        id = parsed_url.path[1:]
        return 'http://%s/data/%s' % (parsed_url.netloc, id)
    else:
        return url

class pycURLContext:

    def __init__(self, url, multi, result_callback):

        self.multi = multi
        self.result_callback = result_callback

        self.curl_ctx = pycurl.Curl()
        self.curl_ctx.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl_ctx.setopt(pycurl.MAXREDIRS, 5)
        self.curl_ctx.setopt(pycurl.CONNECTTIMEOUT, 30)
        self.curl_ctx.setopt(pycurl.TIMEOUT, 300)
        self.curl_ctx.setopt(pycurl.NOSIGNAL, 1)
        self.curl_ctx.setopt(pycurl.URL, str(url))

        self.curl_ctx.ctx = self

    def start(self):
        self.multi.add_fetch(self)

    def success(self):
        self.result_callback(True)
        self.cleanup()

    def failure(self, errno, errmsg):
        self.result_callback(False)
        self.cleanup()

    def cleanup(self):
        self.curl_ctx.close()


class pycURLFetchContext(pycURLContext):

    def __init__(self, dest_fp, src_url, multi, result_callback, progress_callback=None, start_byte=None):

        pycURLContext.__init__(self, src_url, multi, result_callback)

        self.description = src_url
        self.progress_callback = None

        self.curl_ctx.setopt(pycurl.WRITEDATA, dest_fp)
        if progress_callback is not None:
            self.curl_ctx.setopt(pycurl.NOPROGRESS, False)
            self.curl_ctx.setopt(pycurl.PROGRESSFUNCTION, self.progress)
            self.progress_callback = progress_callback
        if start_byte is not None and start_byte != 0:
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-" % start_byte])

    def success(self):
        self.progress_callback(self.curl_ctx.getinfo(pycurl.SIZE_DOWNLOAD))
        pycURLContext.success(self)

    def progress(self, toDownload, downloaded, toUpload, uploaded):
        self.progress_callback(downloaded)

class pycURLPostContext(pycURLContext):

    def __init__(self, in_fp, in_length, out_fp, url, multi, result_callback):
        
        pycURLContext.__init__(self, url, multi, result_callback)

        self.read_fp = in_fp
        self.write_fp = out_fp

        self.curl_ctx.setopt(pycurl.WRITEFUNCTION, self.write)
        self.curl_ctx.setopt(pycurl.READFUNCTION, self.read)
        self.curl_ctx.setopt(pycurl.POST, True)
        self.curl_ctx.setopt(pycurl.POSTFIELDSIZE, in_length)
        self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Content-Type: application/octet-stream"])

    def write(self, data):
        self.write_fp.write(data)
        return len(data)

    def read(self, chars):
        return self.read_fp.read(chars)

class SelectableEventQueue:

    def set_fd_nonblocking(self, fd):
        oldflags = fcntl.fcntl(fd, fcntl.F_GETFL)
        newflags = oldflags | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, newflags)

    def __init__(self):
        self._lock = threading.Lock()
        self.event_pipe_read, self.event_pipe_write = os.pipe()
        self.set_fd_nonblocking(self.event_pipe_read)
        self.set_fd_nonblocking(self.event_pipe_write)
        self.event_queue = []

    def drain_event_pipe(self):
        try:
            while(len(os.read(self.event_pipe_read, 1024)) >= 0):
                pass
        except OSError, e:
            if e.errno == EAGAIN:
                return
            else:
                raise

    def notify_event(self):
        try:
            os.write(self.event_pipe_write, "X")
        except OSError, e:
            if e.errno == EAGAIN:
                # Event pipe is full -- that's fine, the thread will wake next time it selects.
                return
            else:
                raise

    def post_event(self, ev):
        with self._lock:
            self.event_queue.append(ev)
            self.notify_event()

    def dispatch_events(self):
        with self._lock:
            ret = (len(self.event_queue) > 0)
            for event in self.event_queue:
                event()
            self.event_queue = []
            self.drain_event_pipe()
            return ret

    def get_select_fds(self):
        return [self.event_pipe_read], [], []

    # Called after all event-posting and dispatching is complete
    def cleanup(self):
        os.close(self.event_pipe_read)
        os.close(self.event_pipe_write)

class pycURLThread:

    def __init__(self):
        self.thread = None
        self.curl_ctx = pycurl.CurlMulti()
        self.curl_ctx.setopt(pycurl.M_PIPELINING, 1)
        self.curl_ctx.setopt(pycurl.M_MAXCONNECTS, 20)
        self.active_fetches = []
        self.event_queue = SelectableEventQueue()
        self.dying = False

    def start(self):
        self.thread = threading.Thread(target=self.pycurl_main_loop)
        self.thread.start()

    # Called from cURL thread
    def add_fetch(self, new_context):
        self.active_fetches.append(new_context)
        self.curl_ctx.add_handle(new_context.curl_ctx)

    def do_from_curl_thread(self, callback):
        self.event_queue.post_event(callback)

    def call_and_signal(self, callback, e, ret):
        ret.ret = callback()
        e.set()

    class ReturnBucket:
        def __init__(self):
            self.ret = None

    def do_from_curl_thread_sync(self, callback):
        e = threading.Event()
        ret = pycURLThread.ReturnBucket()
        self.event_queue.post_event(lambda: call_and_signal(callback, e, ret))
        e.wait()
        return ret.ret

    def _stop_thread(self):
        self.dying = True
    
    def stop(self):
        self.event_queue.post_event(self._stop_thread)

    def pycurl_main_loop(self):
        while True:
            # Curl-perform and process events until there's nothing left to do
            while True:
                go_again = False
                # Perform until cURL has nothing left to do
                while True:
                    ret, num_handles = self.curl_ctx.perform()
                    if ret != pycurl.E_CALL_MULTI_PERFORM:
                        break
                # Fire callbacks on completed fetches
                while True:
                    num_q, ok_list, err_list = self.curl_ctx.info_read()
                    if len(ok_list) > 0 or len(err_list) > 0:
                        go_again = True
                    for c in ok_list:
                        self.curl_ctx.remove_handle(c)
                        response_code = c.getinfo(pycurl.RESPONSE_CODE)
#                        ciel.log.error("Curl success: %s -- %s" % (c.ctx.description, str(response_code)))
                        if str(response_code).startswith("2"):
                            c.ctx.success()
                        else:
                            ciel.log.error("Curl failure: HTTP %s" % str(response_code), "CURL_FETCH", logging.WARNING)
                            c.ctx.failure(response_code, "")
                        self.active_fetches.remove(c.ctx)
                    for c, errno, errmsg in err_list:
                        self.curl_ctx.remove_handle(c)
                        ciel.log.error("Curl failure: %s, %s" % 
                                           (str(errno), str(errmsg)), "CURL_FETCH", logging.WARNING)
                        c.ctx.failure(errno, errmsg)
                        self.active_fetches.remove(c.ctx)
                    if num_q == 0:
                        break
                # Process events, both from out-of-thread and due to callbacks
                if self.event_queue.dispatch_events():
                    go_again = True
                if self.dying:
                    return
                if not go_again:
                    break
            if self.dying:
                return
            # Alright, all work appears to be done for now. Gather reasons to wake up.
            # Reason #1: cURL has work to do.
            read_fds, write_fds, exn_fds = self.curl_ctx.fdset()
            # Reason #2: out-of-thread events arrived.
            ev_rfds, ev_wfds, ev_exfds = self.event_queue.get_select_fds()
            read_fds.extend(ev_rfds)
            write_fds.extend(ev_wfds)
            exn_fds.extend(ev_exfds)
            active_read, active_write, active_exn = select.select(read_fds, write_fds, exn_fds)

class FileTransferContext:

    def __init__(self, urls, save_filename, multi, callbacks):
        self.urls = urls
        self.multi = multi
        self.save_filename = save_filename
        self.callbacks = callbacks
        self.failures = 0

    def start_next_attempt(self):
        self.fp = open(self.save_filename, "w")
        ciel.log("Starting fetch attempt %d using %s" % (self.failures + 1, self.urls[self.failures]), "CURL_FETCH", logging.INFO)
        self.curl_fetch = pycURLFetchContext(self.fp, self.urls[self.failures], self.multi, self.result, self.callbacks.progress)
        self.curl_fetch.start()

    def start(self):
        self.start_next_attempt()

    def result(self, success):
        self.fp.close()
        if success:
            self.callbacks.result(True)
        else:
            self.failures += 1
            if self.failures == len(self.urls):
                ciel.log.error('No more URLs to try.', 'BLOCKSTORE', logging.ERROR)
                self.callbacks.result(False)
            else:
                ciel.log.error("Fetch %s to %s failed; trying next URL" % (self.urls[self.failures - 1], self.save_filename))
                self.callbacks.reset()
                self.start_next_attempt()

class StreamTransferContext:

    def __init__(self, ref, block_store, callbacks):
        self.url = block_store.get_fetch_urls_for_ref(ref)[0]
        parsed_url = urlparse.urlparse(self.url)
        self.worker_netloc = parsed_url.netloc
        self.ref = ref
        self.save_filename = block_store.streaming_filename(ref.id)
        self.fp = open(self.save_filename, "w")
        self.callbacks = callbacks
        self.current_data_fetch = None
        self.previous_fetches_bytes_downloaded = 0
        self.remote_done = False
        self.remote_failed = False
        self.latest_advertisment = 0
        self.block_store = block_store

    def start_next_fetch(self):
        ciel.log("Stream-fetch %s: start fetch" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.current_data_fetch = pycURLFetchContext(self.fp, self.url, self.block_store.fetch_thread, self.result, self.progress, self.previous_fetches_bytes_downloaded)
        self.current_data_fetch.start()

    def start(self):
        
        ciel.log("Starting stream-fetch for %s" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.start_next_fetch()
        ciel.log("Stream-fetch %s: accepting advertisments" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.block_store.add_incoming_stream(self.ref.id, self)
        post_data = simplejson.dumps({"netloc": self.block_store.netloc})
        self.block_store._post_string_noreturn("http://%s/control/streamstat/%s/subscribe" % (self.worker_netloc, self.ref.id), post_data)
        ciel.log("Stream-fetch %s: subscribed to advertisments from %s" % (self.ref.id, self.worker_netloc), "CURL_FETCH", logging.INFO)

    def progress(self, bytes_downloaded):
        self.callbacks.progress(self.previous_fetches_bytes_downloaded + bytes_downloaded)

    def consider_next_fetch(self):
        if self.remote_done or self.latest_advertisment - self.previous_fetches_bytes_downloaded > 67108864:
            self.start_next_fetch()
        else:
            ciel.log("Stream-fetch %s: paused (remote has %d, I have %d)" % 
                     (self.ref.id, self.latest_advertisment, self.previous_fetches_bytes_downloaded), 
                     "CURL_FETCH", logging.INFO)
            self.current_data_fetch = None

    def check_complete(self):
        if self.remote_done and self.latest_advertisment == self.previous_fetches_bytes_downloaded:
            ciel.log("Stream-fetch %s: complete" % self.ref.id, "CURL_FETCH", logging.INFO)
            self.complete(True)
        else:
            self.consider_next_fetch()

    def result(self, success):
        # Current transfer finished.
        if self.remote_failed:
            # advertisment subscription failed
            ciel.log("Stream-fetch %s: advertisment subscription failed. Failing transfer." % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.complete(False)
            return
        if not success:
            ciel.log("Stream-fetch %s: transfer failed" % self.ref.id)
            self.complete(False)
        else:
            this_fetch_bytes = self.current_data_fetch.curl_ctx.getinfo(pycurl.SIZE_DOWNLOAD)
            ciel.log("Stream-fetch %s: transfer succeeded (got %d bytes)" % (self.ref.id, this_fetch_bytes),
                     "CURL_FETCH", logging.INFO)
            self.previous_fetches_bytes_downloaded += this_fetch_bytes
            self.check_complete()

    def complete(self, success):
        self.fp.close()
        self.block_store.remove_incoming_stream(self.ref.id)
        self.callbacks.result(success)

    def advertisment(self, bytes=None, done=None, absent=None):
        if absent is True:
            ciel.log("Stream-fetch %s: advertisment subscription reported file absent" % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.remote_failed = True
            if self.current_data_fetch is None:
                self.complete(False)
        else:
            ciel.log("Stream-fetch %s: got advertisment: bytes %d done %s" % (self.ref.id, bytes, done), "CURL_FETCH", logging.INFO)
            self.latest_advertisment = bytes
            self.remote_done = done
            if self.current_data_fetch is None:
                self.check_complete()

class BlockStore(plugins.SimplePlugin):

    def __init__(self, bus, hostname, port, base_dir, ignore_blocks=False):
        plugins.SimplePlugin.__init__(self, bus)
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
        self.bus = bus
        self.fetch_thread = pycURLThread()
        self.dataval_codec = codecs.lookup("string_escape")
    
        self.pin_set = set()
    
        self.ignore_blocks = ignore_blocks
    
        # Maintains a set of block IDs that are currently being written.
        # (i.e. They are in the pre-publish/streamable state, and have not been
        #       committed to the block store.)
        # They map to the executor which is producing them.
        self.streaming_producers = dict()

        # The other side of the coin: things we're streaming *in*
        self.incoming_streams = dict()

        # Things we're fetching. The streams dictionary above maps to StreamTransferContexts for advertisment delivery;
        # this maps to FetchListeners for clients to attach and get progress notifications.
        self.incoming_fetches = dict()

        # Block IDs that are held locally and are complete
        self.local_blocks = set()
            
        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle, 'handle': self.decode_handle, 'script': self.decode_script}

    def start(self):
        self.fetch_thread.start()

    def stop(self):
        self.fetch_thread.stop()

    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop, 10)

    def decode_handle(self, file):
        return file
    def decode_script(self, file):
        return CloudScriptParser().parse(file.read())
    def encode_noop(self, obj, file):
        return file.write(obj)
    def decode_noop(self, file):
        return file.read()    
    def encode_json(self, obj, file):
        return simplejson.dump(obj, file, cls=SWReferenceJSONEncoder)
    def decode_json(self, file):
        return simplejson.load(file, object_hook=json_decode_object_hook)
    def encode_pickle(self, obj, file):
        return pickle.dump(obj, file)
    def decode_pickle(self, file):
        return pickle.load(file)
    
    def allocate_new_id(self):
        return str(uuid.uuid1())
    
    def pin_filename(self, id): 
        return os.path.join(self.base_dir, PIN_PREFIX + id)
    
    def streaming_filename(self, id):
        return os.path.join(self.base_dir, '.%s' % id)
    
    def filename(self, id):
        return os.path.join(self.base_dir, str(id))
        
    class FileOutputContext:

        def __init__(self, refid, block_store):
            self.refid = refid
            self.block_store = block_store
            self.closed = False

        def get_filename(self):
            return self.block_store.streaming_filename(self.refid)

        def get_stream_ref(self):
            return SW2_StreamReference(self.refid, [self.block_store.netloc])

        def rollback(self):
            self.block_store.rollback_file(self.refid)

        def close(self):
            self.closed = True
            self.block_store.commit_stream(self.refid)

        def get_completed_ref(self):
            if not self.closed:
                raise Exception("FileOutputContext for ref %s must be closed before it is realised as a concrete reference" % self.refid)
            completed_file = self.block_store.filename(self.refid)
            file_size = os.stat(completed_file).st_size
            if file_size < 1024:
                with open(completed_file, "r") as fp:
                    return SWDataValue(self.refid, self.block_store.encode_datavalue(fp.read()))
            else:
                return SW2_ConcreteReference(self.refid, size_hint=file_size, location_hints=[self.block_store.netloc])

        def __enter__(self):
            return self

        def __exit__(self, exnt, exnv, exntb):
            if exnt is None:
                self.close()
            else:
                ciel.log("FileOutputContext %s destroyed due to exception %s: rolling back" % (self.refid, exnv), "BLOCKSTORE", logging.WARNING)
                self.rollback()
            return False

    def make_local_output(self, id, executor=None):
        '''
        Creates a file-in-progress in the block store directory.
        '''
        ciel.log.error('Creating file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        with self._lock:
            if id in self.local_blocks:
                ciel.log("Block %s already existed! Overwriting..." % id, "BLOCKSTORE", logging.WARNING)
                self.local_blocks.discard(id)
                os.remove(self.filename(id))
            self.streaming_producers[id] = executor
            dot_filename = self.streaming_filename(id)
            open(dot_filename, 'wb').close()
        return BlockStore.FileOutputContext(id, self)

    def commit_stream(self, id):
        ciel.log.error('Committing file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        os.link(self.streaming_filename(id), self.filename(id))
        with self._lock:
            del self.streaming_producers[id]
            self.local_blocks.add(id)

    def rollback_file(self, id):
        ciel.log.error('Rolling back streamed file for output %s' % id, 'BLOCKSTORE', logging.WARNING)
        with self._lock:
            del self.streaming_producers[id]
            os.unlink(self.streaming_filename(id))

    def ref_from_string(self, string, id):
        output_ctx = self.make_local_output(id)
        with open(output_ctx.get_filename(), "w") as fp:
            fp.write(string)
        output_ctx.close()
        return output_ctx.get_completed_ref()

    def cache_object(self, object, encoder, id):
        self.object_cache[(id, encoder)] = object        

    def ref_from_object(self, object, encoder, id):
        """Encodes an object, returning either a DataValue or ConcreteReference as appropriate"""
        self.cache_object(object, encoder, id)
        buffer = StringIO()
        self.encoders[encoder](object, buffer)
        ret = self.ref_from_string(buffer.getvalue(), id)
        buffer.close()
        return ret

    # Why not just rename to self.filename(id) and skip this nonsense? Because os.rename() can be non-atomic.
    # When it renames between filesystems it does a full copy; therefore I copy/rename to a colocated dot-file,
    # then complete the job by linking the proper name in output_ctx.close().
    def ref_from_external_file(self, filename, id):
        output_ctx = self.make_local_output(id)
        shutil.move(filename, output_ctx.get_filename())
        output_ctx.close()
        return output_ctx.get_completed_ref()

    # Remote is subscribing to updates from one of our streaming producers
    def subscribe_to_stream(self, otherend_netloc, id):
        post = None
        with self._lock:
            try:
                self.streaming_producers[id].subscribe_output(otherend_netloc, id)
            except KeyError:
                if id in self.local_blocks:
                    st = os.stat(self.filename(id))
                    post = simplejson.dumps({"bytes": st.st_size, "done": True})
                else:
                    post = simplejson.dumps({"absent": True})
        if post is not None:
            self.post_string_noreturn("http://%s/control/streamstat/%s/advert" % (otherend_netloc, id), post)

    def encode_datavalue(self, str):
        return (self.dataval_codec.encode(str))[0]

    def decode_datavalue(self, ref):
        return (self.dataval_codec.decode(ref.value))[0]

    # Called from cURL thread
    def add_incoming_stream(self, id, transfer_ctx):
        self.incoming_streams[id] = transfer_ctx

    # Called from cURL thread
    def remove_incoming_stream(self, id):
        del self.incoming_streams[id]

    # Called from cURL thread
    def _receive_stream_advertisment(self, id, **args):
        try:
            self.incoming_streams[id].advertisment(**args)
        except KeyError:
            ciel.log("Got advertisment for %s which is not an ongoing stream" % id, "BLOCKSTORE", logging.WARNING)
            pass

    def receive_stream_advertisment(self, id, **args):
        self.fetch_thread.do_from_curl_thread(lambda: self._receive_stream_advertisment(id, **args))
        
    def is_ref_local(self, ref):
        assert isinstance(ref, SWRealReference)

        if isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()

        with self._lock:
            if ref.id in self.local_blocks:
                return True
            if isinstance(ref, SWDataValue):
                with open(self.filename(ref.id), 'w') as obj_file:
                    obj_file.write(self.decode_datavalue(ref))
                self.local_blocks.add(ref.id)
                return True

        return False

    class FetchListener:
        
        def __init__(self, ref, block_store):
            self.progress_listeners = set()
            self.result_listeners = set()
            self.reset_listeners = set()
            self.last_progress = 0
            self.ref = ref
            self.block_store = block_store

        def progress(self, bytes):
            for callback in self.progress_listeners:
                callback(bytes)
            self.last_progress = bytes

        def result(self, success):
            self.block_store.fetch_completed(self.ref, success)
            for callback in self.result_listeners:
                callback(success)

        def reset(self):
            for callback in self.reset_listeners:
                callback()

        def add_listener(self, result_cb, reset_cb, progress_cb):
            self.result_listeners.add(result_cb)
            self.reset_listeners.add(reset_cb)
            if progress_cb is not None:
                self.progress_listeners.add(progress_cb)
                progress_cb(self.last_progress)

    # Called from cURL thread
    # After this method completes, the ref's streaming_filename must exist.
    def _start_fetch_ref(self, ref):
            
        new_listener = BlockStore.FetchListener(ref, self)
        self.incoming_fetches[ref.id] = new_listener
        urls = self.get_fetch_urls_for_ref(ref)
        save_filename = self.streaming_filename(ref.id)
        if isinstance(ref, SW2_ConcreteReference):
            ctx = FileTransferContext(urls, save_filename, self.fetch_thread, new_listener)
        elif isinstance(ref, SW2_StreamReference):
            ctx = StreamTransferContext(ref, self, new_listener)
        ctx.start()

    # Called from cURL thread
    def fetch_completed(self, ref, success):
        if success:
            with self._lock:
                # local_blocks can be accessed from any thread.
                os.link(self.streaming_filename(ref.id), self.filename(ref.id))
                self.local_blocks.add(ref.id)
        del self.incoming_fetches[ref.id]

    # Called from cURL thread
    def _fetch_ref_async(self, ref, fetch_context, result_callback, reset_callback, progress_callback):
        
        if self.is_ref_local(ref):
            ciel.log("Ref %s became local during thread-switch" % ref, "BLOCKSTORE", logging.INFO)
            fetch_context.fetch_completed()
            result_callback(True)
        else:
            # No locking from now on, as the following structures are only touched by the cURL thread.
            if ref.id not in self.incoming_fetches:
                ciel.log("Starting new fetch for ref %s" % ref, "BLOCKSTORE", logging.INFO)
                self._start_fetch_ref(ref)
                fetch_context.fetch_in_progress()
            else:
                ciel.log("Joining existing fetch for ref %s" % ref, "BLOCKSTORE", logging.INFO)
            self.incoming_fetches[ref.id].add_listener(result_callback, reset_callback, progress_callback)

    class CompletedFetch:

        def __init__(self, filename):
            self.filename = filename

        def get_filename(self):
            return self.filename

    class FetchInProgress:
        
        def __init__(self, completed_filename, in_progress_filename):
            self.ready_event = threading.Event()
            self.completed_filename = completed_filename
            self.in_progress_filename = in_progress_filename
            self.ret_filename = None
            
        def fetch_completed(self):
            self.ret_filename = self.completed_filename
            self.ready_event.set()

        def fetch_in_progress(self):
            self.ret_filename = self.in_progress_filename
            self.ready_event.set()

        def get_filename(self):
            self.ready_event.wait()
            return self.ret_filename

    # Called from arbitrary thread
    def fetch_ref_async(self, ref, result_callback, reset_callback, progress_callback=None):

        if self.is_ref_local(ref):
            ciel.log("Ref %s already local; no fetch required" % ref, "BLOCKSTORE", logging.INFO)
            result_callback(True)
            return BlockStore.CompletedFetch(self.filename(ref.id))
        else:
            new_ctx = BlockStore.FetchInProgress(self.filename(ref.id), self.streaming_filename(ref.id))
            self.fetch_thread.do_from_curl_thread(lambda: self._fetch_ref_async(ref, new_ctx, result_callback, reset_callback, progress_callback))
            return new_ctx

    class SynchronousTransfer:
        
        def __init__(self, ref):
            self.ref = ref
            self.finished_event = threading.Event()

        def result(self, success):
            self.success = success
            self.finished_event.set()

        def reset(self):
            pass

        def wait(self):
            self.finished_event.wait()

    def retrieve_filenames_for_refs(self, refs):
        
        ctxs = []
        for ref in refs:
            sync_transfer = BlockStore.SynchronousTransfer(ref)
            ciel.log("Synchronous fetch ref %s" % ref, "BLOCKSTORE", logging.INFO)
            transfer_ctx = self.fetch_ref_async(ref, sync_transfer.result, sync_transfer.reset)
            ctxs.append(sync_transfer)
            
        for ctx in ctxs:
            ctx.wait()
            
        failed_transfers = filter(lambda x: not x.success, ctxs)
        if len(failed_transfers) > 0:
            raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints)) for ctx in failed_transfers]))
        return [self.filename(ref.id) for ref in refs]

    def retrieve_filename_for_ref(self, ref):

        return self.retrieve_filenames_for_refs([ref])[0]

    def retrieve_strings_for_refs(self, refs):

        strs = []
        files = self.retrieve_filenames_for_refs(refs)
        for fname in files:
            with open(fname, "r") as fp:
                strs.append(fp.read())
        return strs

    def retrieve_string_for_ref(self, ref):
        
        return self.retrieve_strings_for_refs([ref])[0]

    def retrieve_objects_for_refs(self, ref_and_decoders):

        solutions = dict()
        unsolved_refs = []
        for (ref, decoder) in ref_and_decoders:
            try:
                solutions[ref.id] = self.object_cache[(ref.id, decoder)]
            except:
                unsolved_refs.append(ref)

        strings = self.retrieve_strings_for_refs(unsolved_refs)
        str_of_ref = dict([(ref.id, string) for (string, ref) in zip(strings, unsolved_refs)])
            
        for (ref, decoder) in ref_and_decoders:
            if ref.id not in solutions:
                decoded = self.decoders[decoder](StringIO(str_of_ref[ref.id]))
                self.object_cache[(ref.id, decoder)] = decoded
                solutions[ref.id] = decoded
            
        return [solutions[ref.id] for (ref, decoder) in ref_and_decoders]

    def retrieve_object_for_ref(self, ref, decoder):
        
        return self.retrieve_objects_for_refs([(ref, decoder)])[0]

    def get_fetch_urls_for_ref(self, ref):

        if isinstance(ref, SW2_ConcreteReference):
            return ["http://%s/data/%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
        elif isinstance(ref, SW2_StreamReference):
            return ["http://%s/data/.%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
        elif isinstance(ref, SW2_FetchReference):
            return [ref.url]
                
    def get_ref_for_url(self, url, version, task_id):
        """
        Returns a SW2_ConcreteReference for the data stored at the given URL.
        Currently, the version is ignored, but we imagine using this for e.g.
        HTTP ETags, which would raise an error if the data changed.
        """
        
        parsed_url = urlparse.urlparse(url)
        if parsed_url.scheme == 'swbs':
            # URL is in a Skywriting Block Store, so we can make a reference
            # for it directly.
            id = parsed_url.path[1:]
            ref = SW2_ConcreteReference(id, None)
            ref.add_location_hint(parsed_url.netloc)
        else:
            # URL is outside the cluster, so we have to fetch it. We use
            # content-based addressing to name the fetched data.
            hash = hashlib.sha1()
            
            # 1. Fetch URL to a file-like object.
            with contextlib.closing(urllib2.urlopen(url)) as url_file:
            
                # 2. Hash its contents and write it to disk.
                with tempfile.NamedTemporaryFile('wb', 4096, delete=False) as fetch_file:
                    fetch_filename = fetch_file.name
                    while True:
                        chunk = url_file.read(4096)
                        if not chunk:
                            break
                        hash.update(chunk)
                        fetch_file.write(chunk)
                
            # 3. Store the fetched file in the block store, named by the
            #    content hash.
            id = 'urlfetch:%s' % hash.hexdigest()
            ref = self.ref_from_external_file(fetch_filename, id)

        return ref
        
    def choose_best_netloc(self, netlocs):
        for netloc in netlocs:
            if netloc == self.netloc:
                return netloc
        return random.choice(list(netlocs))
        
    def choose_best_url(self, urls):
        if len(urls) == 1:
            return urls[0]
        else:
            for url in enumerate(urls):
                parsed_url = urlparse.urlparse(url)
                if parsed_url.netloc == self.netloc:
                    return url
            return random.choice(urls)

    class PostContext:
        
        def __init__(self, url, postdata, fetch_thread):

            self.post_buffer = StringIO(postdata)
            self.response_buffer = StringIO()
            self.completed_event = threading.Event()
            self.curl_ctx = pycURLPostContext(self.post_buffer, len(postdata), self.response_buffer, url, fetch_thread, self.result)

        def start(self):

            self.curl_ctx.start()

        def get_result(self):

            self.completed_event.wait()
            if self.response_string is not None:
                return self.response_string
            else:
                raise Exception("Curl-post failed")

        def result(self, success):
            
            if success:
                self.response_string = self.response_buffer.getvalue()
            else:
                self.response_string = None
            self.post_buffer.close()
            self.response_buffer.close()
            self.completed_event.set()

    # This is only a BlockStore method because it uses the fetch_thread.
    # Called from cURL thread
    def _post_string_noreturn(self, url, postdata):
        ctx = BlockStore.PostContext(url, postdata, self.fetch_thread)
        ctx.start()
        return

    def post_string_noreturn(self, url, postdata):
        self.fetch_thread.do_from_curl_thread(lambda: self._post_string_noreturn(url, postdata))

    # Called from cURL thread
    def _post_string(self, url, postdata):
        ctx = BlockStore.PostContext(url, postdata, self.fetch_thread)
        ctx.start()
        return ctx.get_result()

    def post_string(self, url, postdata):
        self.fetch_thread.do_from_curl_thread(lambda: self._post_string(url, postdata))

    def find_local_blocks(self):
        ciel.log("Looking for local blocks", "BLOCKSTORE", logging.INFO)
        try:
            for block_name in os.listdir(self.base_dir):
                if block_name.startswith('.'):
                    if not os.path.exists(os.path.join(self.base_dir, block_name[1:])):
                        ciel.log("Deleting incomplete block %s" % block_name, "BLOCKSTORE", logging.WARNING)
                        os.remove(os.path.join(self.base_dir, block_name))
                else:
                    self.local_blocks.add(block_name)
                    ciel.log("Found block %s" % block_name, "BLOCKSTORE", logging.INFO)
        except OSError as e:
            ciel.log("Couldn't enumerate existing blocks: %s" % e, "BLOCKSTORE", logging.WARNING)

    def block_list_generator(self):
        ciel.log.error('Generating block list for local consumption', 'BLOCKSTORE', logging.INFO)
        for block_name in os.listdir(self.base_dir):
            if not block_name.startswith('.'):
                block_size = os.path.getsize(os.path.join(self.base_dir, block_name))
                yield block_name, block_size
    
    def build_pin_set(self):
        ciel.log.error('Building pin set', 'BLOCKSTORE', logging.INFO)
        initial_size = len(self.pin_set)
        for filename in os.listdir(self.base_dir):
            if filename.startswith(PIN_PREFIX):
                self.pin_set.add(filename[len(PIN_PREFIX):])
                ciel.log.error('Pinning block %s' % filename[len(PIN_PREFIX):], 'BLOCKSTORE', logging.INFO)
        ciel.log.error('Pinned %d new blocks' % (len(self.pin_set) - initial_size), 'BLOCKSTORE', logging.INFO)
    
    def generate_block_list_file(self):
        ciel.log.error('Generating block list file', 'BLOCKSTORE', logging.INFO)
        with tempfile.NamedTemporaryFile('w', delete=False) as block_list_file:
            filename = block_list_file.name
            for block_name, block_size in self.block_list_generator():
                block_list_file.write(BLOCK_LIST_RECORD_STRUCT.pack(block_name, block_size))
        return filename

    def generate_pin_refs(self):
        ret = []
        for id in self.pin_set:
            ret.append(SW2_ConcreteReference(id, os.path.getsize(self.filename(id)), [self.netloc]))
        return ret

    def pin_ref_id(self, id):
        open(self.pin_filename(id), 'w').close()
        self.pin_set.add(id)
        ciel.log.error('Pinned block %s' % id, 'BLOCKSTORE', logging.INFO)
        
    def flush_unpinned_blocks(self, really=True):
        ciel.log.error('Flushing unpinned blocks', 'BLOCKSTORE', logging.INFO)
        files_kept = 0
        files_removed = 0
        for block_name in os.listdir(self.base_dir):
            if block_name not in self.pin_set and not block_name.startswith(PIN_PREFIX):
                if really:
                    os.remove(os.path.join(self.base_dir, block_name))
                files_removed += 1
            elif not block_name.startswith(PIN_PREFIX):
                files_kept += 1
        if really:
            ciel.log.error('Flushed block store, kept %d blocks, removed %d blocks' % (files_kept, files_removed), 'BLOCKSTORE', logging.INFO)
        else:
            ciel.log.error('If we flushed block store, would keep %d blocks, remove %d blocks' % (files_kept, files_removed), 'BLOCKSTORE', logging.INFO)
        return (files_kept, files_removed)

    def is_empty(self):
        return self.ignore_blocks or len(os.listdir(self.base_dir)) == 0
