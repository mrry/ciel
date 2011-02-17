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

class pycURLFetchContext:

    def __init__(self, dest_fp, src_url, multi, result_callback, progress_callback=None, start_byte=None):

        self.description = src_url
        self.multi = multi

        self.result_callback = result_callback
        self.progress_callback = None

        self.curl_ctx = pycurl.Curl()
        self.curl_ctx.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl_ctx.setopt(pycurl.MAXREDIRS, 5)
        self.curl_ctx.setopt(pycurl.CONNECTTIMEOUT, 30)
        self.curl_ctx.setopt(pycurl.TIMEOUT, 300)
        self.curl_ctx.setopt(pycurl.NOSIGNAL, 1)
        self.curl_ctx.setopt(pycurl.WRITEDATA, dest_fp)
        self.curl_ctx.setopt(pycurl.URL, str(src_url))
        if progress_callback is not None:
            self.curl_ctx.setopt(pycurl.NOPROGRESS, False)
            self.curl_ctx.setopt(pycurl.PROGRESSFUNCTION, self.progress)
            self.progress_callback = progress_callback
        if start_byte is not None:
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-" % start_byte])

        self.curl_ctx.ctx = self

    def start(self):
        self.multi.add_fetch(self)

    def progress(self, toDownload, downloaded, toUpload, uploaded):
        self.progress_callback(downloaded)

    def success(self):
        self.progress_callback(self.curl_ctx.getinfo(SIZE_DOWNLOAD))
        self.result_callback(True)
        self.cleanup()

    def failure(self, errno, errmsg):
        self.result_callback(False)
        self.cleanup()

    def cleanup(self):
        self.curl_ctx.close()

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
        self.contexts = []
        self.active_fetches = []
        self.event_queue = SelectableEventQueue()
        self.dying = False

    def start(self):
        self.thread = threading.Thread(target=self.pycurl_main_loop)
        self.thread.start()

    def _add_fetch(self, new_context):
        self.active_fetches.append(new_context)
        self.curl_ctx.add_handle(new_context.curl_ctx)

    def add_fetch(self, ctx):
        callback_obj = lambda: self._add_fetch(ctx)
        self.event_queue.post_event(callback_obj)

    def _add_context(self, ctx):
        ciel.log.error("Event source registered", "CURL_FETCH", logging.INFO)
        self.contexts.append(ctx)

    def add_context(self, ctx):
        self.event_queue.post_event(lambda: self._add_context(ctx))

    def _remove_context(self, ctx, e):
        ciel.log.error("Event source unregistered", "CURL_FETCH", logging.INFO)
        self.contexts.remove(ctx)
        e.set()

    # Synchronous so that when this call returns the caller definitely will not get any more callbacks
    def remove_context(self, ctx):
        e = threading.Event()
        self.event_queue.post_event(lambda: self._remove_context(ctx, e))
        e.wait()

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
        ret = ReturnBucket()
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
            # Reason #3: one of our contexts has an interesting FD
            for ctx in self.contexts:
                ctx_read, ctx_write, ctx_exn = ctx.get_select_fds()
                read_fds.extend(ctx_read)
                write_fds.extend(ctx_write)
                exn_fds.extend(ctx_exn)
            # Reason #4: one of our contexts wants a callback after a timeout
            select_timeout = None
            def td_secs(td):
                return (float(td.microseconds) / 10**6) + float(td.seconds)
            for ctx in self.contexts:
                ctx_timeout = ctx.get_timeout()
                if ctx_timeout is not None:
                    time_now = datetime.now()
                    if ctx_timeout < time_now:
                        select_timeout = 0
                    else:
                        time_to_wait = td_secs(ctx_timeout - datetime.now())
                        if select_timeout is None or time_to_wait < select_timeout:
                            select_timeout = time_to_wait
            active_read, active_write, active_exn = select.select(read_fds, write_fds, exn_fds, select_timeout)
            for ctx in self.contexts:
                ctx.select_callback(active_read, active_write, active_exn)

# Callbacks for "contexts," things which add event sources for which I need a better name.
class pycURLContextCallbacks:

    def get_select_fds(self):
        return [], [], []

    def get_timeout(self):
        return None

    def select_callback(self, rd, wr, exn):
        pass

class SimpleFileTransferContext:

    def __init__(self, url, multi, result_callback, progress_callback, filename):
        self.fp = open(filename, "w")
        self.result_callback = result_callback
        self.curl_fetch = pycURLFetchContext(fp, url, multi, self.result, progress_callback)

    def start(self):
        self.curl_fetch.start()

    def result(self, success):
        self.fp.close()
        self.result_callback(success)

def create_file_transfer_context(urls, multi, filename, result_callback, reset_callback, progress_callback=None):
    return RetryTransferContext(urls, multi, SimpleFileTransferContext, {"filename": filename}, result_callback, reset_callback, progress_callback)

class BufferTransferContext:

    def __init__(self, url, multi, result_callback, progress_callback, buf):
        self.curl_fetch = pycURLFetchContext(self.buf, url, multi, result_callback, progress_callback)

    def start(self):
        self.curl_fetch.start()

def create_buffer_transfer_context(urls, multi, buf, result_callback, reset_callback):
    return RetryTransferContext(urls, multi, BufferTransferContext, {"buf": buf}, result_callback, reset_callback)

class RetryTransferContext:

    def __init__(self, urls, multi, transfer_class, transfer_args, result_callback, reset_callback=None, progress_callback=None):
        self.urls = urls
        self.filename = filename
        self.multi = multi
        self.failures = 0
        self.result_callback = result_callback
        self.reset_callback = reset_callback
        self.progress_callback = progress_callback
        self.transfer_class = transfer_class
        self.transfer_args = transfer_args

    def start_next_attempt(self):
        self.current_attempt = self.transfer_class(self.urls[failures], multi, self.result, self.progress_callback, **self.transfer_args)
        self.current_attempt.start()

    def start(self):
        self.start_next_attempt()

    def result(self, success):
        if success:
            self.result_callback(True)
        else:
            self.failures += 1
            if self.failures == len(self.urls):
                ciel.log.error('No more URLs to try.', 'BLOCKSTORE', logging.ERROR)
                self.result_callback(False)
            else:
                ciel.log.error("Fetch %s to %s failed; trying next URL" % self.urls[self.failures], self.filename)
                self.reset_callback()
                self.start_next_attempt()

class StreamTransferContext:

    def __init__(self, worker_netloc, refid, filename, multi, block_store, result_callback, progress_callback):
        self.url = "http://%s/data/%s" % (worker_netloc, refid)
        self.worker_netloc = worker_netloc
        self.refid = refid
        self.filename = filename
        self.fp = open(filename, "w")
        self.multi = multi
        self.result_callbacks = set([result_callback])
        self.progress_callbacks = set([progress_callback])
        self.current_data_fetch = None
        self.previous_fetches_bytes_downloaded = 0
        self.remote_done = False
        self.local_done = False
        self.latest_advertisment = 0
        self.block_store = block_store

    def start_next_fetch(self):
        self.current_data_fetch = pycURLFetchContext(self.fp, self.url, self.multi, self.result, self.progress, self.previous_fetches_bytes_downloaded)
        self.current_data_fetch.start()

    def add_stream(self):
        self.block_store.add_incoming_stream(self.refid, self)

    def start(self):
        
        self.start_next_fetch()
        self.multi.do_from_curl_thread(lambda: self.add_stream())
        # TODO: Improve on this blocking RPC by using cURL for this sort of thing
        post_data = simplejson.dumps({"netloc": self.worker_netloc})
        httplib2.Http().request("http://%s/control/streamstat/%s/subscribe" % (self.worker_netloc, self.refid), "POST", post_data)

    def progress(self, bytes_downloaded):
        for callback in self.progress_callbacks:
            callback(self.previous_fetches_bytes_downloaded + bytes_downloaded)

    def consider_next_fetch(self):
        if remote_done or self.latest_advertisment - self.previous_fetches_bytes_downloaded > 67108864:
            self.start_next_fetch()
        else:
            self.current_data_fetch = None

    def result(self, success):
        # Current transfer finished. 
        if not success:
            self.complete(False)
        else:
            this_fetch_bytes = self.current_data_fetch.curl_ctx.getinfo(SIZE_DOWNLOAD)
            self.previous_fetches_bytes_downloaded += this_fetch_bytes
            if self.remote_done and self.latest_advertisment == self.previous_fetches_bytes_downloaded:
                # Complete!
                os.remove("." + self.filename)
                self.complete(True)
            else:
                self.consider_next_fetch()

    def complete(self, success):
        self.fp.close()
        self.local_done = True
        self.block_store.remove_incoming_stream(self.refid)
        for callback in self.result_callbacks:
            self.result_callback(success)

    def advertisment(self, current_bytes_available, file_done):
        self.latest_advertisment = current_bytes_available
        self.remote_done = file_done
        if self.current_data_fetch is None:
            self.consider_next_fetch()

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
    
        self.current_cache_access_id = 0
        self.url_cache_filenames = {}
        self.url_cache_access_times = {}
        
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
        
    def store_raw_file(self, incoming_fobj, id):
        with open(self.filename(id), "wb") as data_file:
            shutil.copyfileobj(incoming_fobj, data_file)
            file_size = data_file.tell()
        return 'swbs://%s/%s' % (self.netloc, str(id)), file_size            
    
    def cache_object(self, object, encoder, id):
        self.object_cache[(id, encoder)] = object        

    def store_object(self, object, encoder, id):
        """Stores the given object as a block, and returns a swbs URL to it."""
        #self.cache_object(object, encoder, id)
        with open(self.filename(id), "wb") as object_file:
            self.encoders[encoder](object, object_file)
            file_size = object_file.tell()
        return 'swbs://%s/%s' % (self.netloc, str(id)), file_size
    
    def store_file(self, filename, id, can_move=False):
        """Stores the file with the given local filename as a block, and returns a swbs URL to it."""
        if can_move:
            shutil.move(filename, self.filename(id))
        else:
            shutil.copyfile(filename, self.filename(id))
        file_size = os.path.getsize(self.filename(id))
        return 'swbs://%s/%s' % (self.netloc, str(id)), file_size

    def ref_from_object(self, object, encoder, id, threshold_bytes=1024):
        """Encodes an object, returning either a DataValue or ConcreteReference as appropriate"""
        with MaybeFile(threshold_bytes=threshold_bytes, filename=self.filename(id)) as maybe_file:
            self.encoders[encoder](object, maybe_file)
            if maybe_file.real_fp is not None:
                # Policy: don't cache the decoded form if the encoded form is big enough to deserve a concrete ref.
                file_size = maybe_file.real_fp.tell()
                ret = SW2_ConcreteReference(id, size_hint=file_size, location_hints=[self.netloc])
            else:
                ret = SWDataValue(id, self.encode_datavalue(maybe_file.fake_fp.getvalue()))
                self.cache_object(object, encoder, id)
        return ret

    def make_stream_sink(self, id, executor):
        '''
        Creates a file-in-progress in the block store directory.
        '''
        ciel.log.error('Prepublishing file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        with self._lock:
            self.streaming_producers[id] = executor
            dot_filename = self.streaming_filename(id)
            open(dot_filename, 'wb').close()
            return dot_filename

    def commit_stream(self, id):
        ciel.log.error('Committing streamed file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        os.link(self.streaming_filename(id), self.filename(id))
        with self._lock:
            del self.streaming_producers[id]

    def rollback_file(self, id):
        ciel.log.error('Rolling back streamed file for output %s' % id, 'BLOCKSTORE', logging.WARNING)
        with self._lock:
            del self.streaming_producers[id]
            os.unlink(self.streaming_filename(id))

    def subscribe_to_stream(self, otherend_netloc, id):
        send_result = False
        with self._lock:
            try:
                self.streaming_producers[id].subscribe_output(otherend_netloc, id)
            except:
                send_result = True
        if send_result:
            try:
                st = os.stat(self.filename(id))
                post = simplejson.dumps({"bytes": st.st_size, "done": True})
            except:
                post = simplejson.dumps({"state": "absent"})
            httplib2.Http().request("http://%s/control/streamstat/%s/advert" % (otherend_netloc, self.refid), "POST", post)

    def encode_datavalue(self, str):
        return (self.dataval_codec.encode(str))[0]

    def decode_datavalue(self, ref):
        return (self.dataval_codec.decode(ref.value))[0]

    # Following three methods: called from cURL thread
    def add_incoming_stream(self, id, transfer_ctx):
        self.incoming_streams[id] = transfer_ctx

    def remove_incoming_stream(self, id):
        del self.incoming_streams[id]

    def _receive_stream_advertisment(self, id, bytes, done):
        try:
            self.incoming_streams[id].advertisment(bytes, done)
        except KeyError:
            pass

    def receive_stream_advertisment(self, id, bytes, done):
        self.fetch_thread.do_from_curl_thread(lambda: self._receive_stream_advertisment(id, bytes, done))
        
    def try_retrieve_filename_for_ref_without_transfer(self, ref):
        assert isinstance(ref, SWRealReference)

        if isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()

        filename = self.filename(ref.id)
        if os.path.exists(filename):
            return filename

        if isinstance(ref, SWDataValue):
            with open(filename, 'w') as obj_file:
                obj_file.write(self.decode_datavalue(ref))
            return filename

        return None

    class DummyFileRetr:

        def __init__(self, filename):
            self.filename = filename
            self.is_streaming = False
            self.completed_event = None

        def commit(self):
            pass

    class FileRetrInProgress:
        
        def __init__(self, block_store, ref, multi, result_callback=None, reset_callback=None, progress_callback=None):
            
            urls = block_store.get_fetch_urls_for_ref(ref)
            self.filename = block_store.streaming_filename(ref.id)
            self.is_streaming = True
            self.commit_filename = block_store.filename(ref.id)
            if isinstance(ref, SW2_ConcreteReference):
                self.ctx = create_file_transfer_context(urls, multi, self.filename, self.result, reset_callback, progress_callback)
            elif isinstance(ref, SW2_StreamReference):
                self.ctx = StreamTransferContext(ref.location_hints[0], ref.id, self.filename, 
                                                 self.fetch_thread, block_store, result_callback, progress_callback)
            self.ref = ref
            self.result_callback = result_callback
            self.block_store = block_store
            self.completion_event = threading.Event()

        def start(self):
            self.ctx.start()
            
        def result(self, success):
            if not success:
                self.filename = None
            self.completion_event.set()
            if self.result_callback is not None:
                self.result_callback(success)

        def commit(self):
            if self.filename is not None:
                os.rename(self.filename, self.commit_filename)

    def retrieve_filename_for_ref_async(self, ref, result_callback=None, reset_callback=None, progress_callback=None):

        filename = self.try_retrieve_filename_for_ref_without_transfer(ref)
        if filename is not None:
            if result_callback is not None:
                result_callback(True)
            return DummyFileRetr(filename)
        else:
            if os.path.exists(self.streaming_filename(ref.id)):
                raise Exception("Local stream-joining support not implemented yet")
            ret = FileRetrInProgress(ref, self.filename(ref.id), self.get_fetch_urls_for_ref(ref), self.fetch_thread, 
                                     result_callback, reset_callback, progress_callback)
            ret.start()
            return ret

    def retrieve_filenames_for_refs(self, refs):
        
        transfer_ctxs = [self.retrieve_filename_for_ref_async(ref) for ref in refs]
        self.await_async_transfers(transfer_ctxs)
        failed_transfers = filter(lambda x: x.filename is None, transfer_ctxs)
        if len(failed_transfers) > 0:
            raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints))]))
        return [ctx.filename for ctx in transfer_ctxs]

    def retrieve_filename_for_ref(self, ref):

        return self.retrieve_filenames_for_refs([ref])

    def try_retrieve_string_for_ref_without_transfer(self, ref):
        assert isinstance(ref, SWRealReference)

        if isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()
        elif isinstance(ref, SWDataValue):
            return self.decode_datavalue(ref)
        elif isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference):
            to_read = self.filename(ref.id)
            if os.path.exists(to_read):
                with open(to_read, "r") as f:
                    return f.read()
            else:
                return None

    class DummyStringRetr:

        def __init__(self, str):
            self.str = str
            self.completed_event = None

    class StringRetrInProgress:

        def __init__(self, ref, urls, multi):
            self.buf = StringIO()
            self.ref = ref
            self.urls = urls
            self.transfer = create_buffer_transfer_context(urls, multi, self.buf, self.result, self.reset)
            self.completed_event = threading.Event()
            self.str = None

        def start(self):
            self.transfer.start()

        def result(self, success):
            self.success = success
            if success:
                self.str = self.buf.getvalue()
            self.buf.close()
            self.completed_event.set()

        def reset(self):
            self.buf.truncate(0)

    def retrieve_string_for_ref_async(self, ref):
        str = try_retrieve_string_for_ref_without_transfer(ref)
        if str is not None:
            return DummyStringRetr(str)
        else:
            ret = StringRetrInProgress(ref, self.get_fetch_urls_for_ref(ref), self.fetch_thread)
            return ret

    def retrieve_string_for_ref(self, ref):
        ctx = retrieve_string_for_ref_async(ref)
        self.await_async_transfers([ctx])
        if ctx.str is None:
            raise MissingInputException({ref.id: SW2_TombstoneReference(ref.id, ref.location_hints)})
        return ctx.str

    def try_retrieve_object_for_ref_without_transfer(self, ref, decoder):

        # Basically like the same task for files, but with the possibility of cached decoded forms
        try:
            return self.object_cache[(ref.id, decoder)]
        except:
            pass
        str = self.try_retrieve_string_for_ref_without_transfer(ref)
        if str is not None:
            decoded = self.decoders[decoder](StringIO(str))
            self.object_cache[(ref.id, decoder)] = decoded
            return decoded
        return None

    class DummyObjRetr:

        def __init__(self, obj):
            self.obj = obj
            self.completed_event = None

        def get_obj(self):
            return self.obj

    class ObjRetrInProgress:
        
        def __init__(self, ref, urls, decoder, multi):
            self.string_transfer = StringTransferInProgress(ref, urls, multi)
            self.completed_event = self.string_transfer.completed_event
            self.decoder = decoder

        def start(self):
            self.string_transfer.start()

        def get_obj(self):
            return self.decoder(StringIO(self.string_transfer.str))

    def retrieve_object_for_ref_async(self, ref, decoder):
        obj = self.try_retrieve_object_for_ref_without_transfer(ref, decoder)
        if obj is not None:
            return DummyObjRetr(obj)
        else:
            ret = ObjRetrInProgress(ref, self.get_fetch_urls_for_ref(ref), self.decoders[decoder], self.fetch_thread)
            ret.start()

    def retrieve_object_for_ref(self, ref, decoder):
        ctx = self.retrieve_object_for_ref_async(ref, decoder)
        self.await_async_transfers([ctx])
        return ctx.get_obj()

    def await_async_transfers(self, transfers):
        for transfer in transfers:
            if transfer.completed_event is not None:
                transfer.completed_event.wait()

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
            _, size = self.store_file(fetch_filename, id, True)
            
            ref = SW2_ConcreteReference(id, size)
            ref.add_location_hint(self.netloc)
        
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
