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
import shutil
import pickle
import os
import uuid
import struct
import tempfile
import cherrypy
import logging
import pycurl
import select
import fcntl
import re
import threading
from datetime import datetime, timedelta
import time
from cStringIO import StringIO
from errno import EAGAIN, EPIPE
from cherrypy.process import plugins

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from skywriting.runtime.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SWURLReference, \
    SW2_StreamReference,\
    SW2_TombstoneReference, SW2_FetchReference, SWReferenceJSONEncoder
import hashlib
import contextlib
from skywriting.lang.parser import CloudScriptParser
import skywriting
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

    def __init__(self, callback_obj, url, range=None):

        self.description = url

        self.curl_ctx = pycurl.Curl()
        self.curl_ctx.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl_ctx.setopt(pycurl.MAXREDIRS, 5)
        self.curl_ctx.setopt(pycurl.CONNECTTIMEOUT, 30)
        self.curl_ctx.setopt(pycurl.TIMEOUT, 300)
        self.curl_ctx.setopt(pycurl.NOSIGNAL, 1)
        self.curl_ctx.setopt(pycurl.WRITEFUNCTION, callback_obj.write_data)
        self.curl_ctx.setopt(pycurl.HEADERFUNCTION, callback_obj.write_header_line)
        self.curl_ctx.setopt(pycurl.URL, str(url))
        self.curl_ctx.ctx = self
        if range is not None:
            (start_range, end_range) = range
            if end_range is None:
                self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-" % start_range])
            else:
                self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-%d" % range])

        self.callbacks = callback_obj
            
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

    def _add_fetch(self, callbacks, url, range):
        new_context = pycURLFetchContext(callbacks, url, range)
        self.active_fetches.append(new_context)
        self.curl_ctx.add_handle(new_context.curl_ctx)

    def add_fetch(self, callbacks, url, range=None):
        callback_obj = lambda: self._add_fetch(callbacks, url, range)
        self.event_queue.post_event(callback_obj)

    def _add_context(self, ctx):
        cherrypy.log.error("Event source registered", "CURL_FETCH", logging.INFO)
        self.contexts.append(ctx)

    def add_context(self, ctx):
        self.event_queue.post_event(lambda: self._add_context(ctx))

    def _remove_context(self, ctx, e):
        cherrypy.log.error("Event source unregistered", "CURL_FETCH", logging.INFO)
        self.contexts.remove(ctx)
        e.set()

    # Synchronous so that when this call returns the caller definitely will not get any more callbacks
    def remove_context(self, ctx):
        e = threading.Event()
        self.event_queue.post_event(lambda: self._remove_context(ctx, e))
        e.wait()

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
#                        cherrypy.log.error("Curl success: %s -- %s" % (c.ctx.description, str(response_code)))
                        if str(response_code).startswith("2"):
                            c.ctx.callbacks.success()
                        else:
                            c.ctx.callbacks.failure(response_code, "")
                        c.ctx.cleanup()
                        self.active_fetches.remove(c.ctx)
                    for c, errno, errmsg in err_list:
                        self.curl_ctx.remove_handle(c)
                        cherrypy.log.error("Curl failure: %s, %s" % 
                                           (str(errno), str(errmsg)), "CURL_FETCH", logging.WARNING)
                        c.ctx.callbacks.failure(errno, errmsg)
                        c.ctx.cleanup()
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

# Callbacks for a single fetch
class pycURLFetchCallbacks:

    def write_data(self, str):
        pass

    def write_header_line(self, str):
        pass
    
    def success(self):
        pass

    def failure(self, errno, errstr):
        pass

# Callbacks for "contexts," things which add event sources for which I need a better name.
class pycURLContextCallbacks:

    def get_select_fds(self):
        return [], [], []

    def get_timeout(self):
        return None

    def select_callback(self, rd, wr, exn):
        pass

class RetryTransferContext(pycURLFetchCallbacks):

    def __init__(self, urls, multi, done_callback):
        self.urls = urls
        self.multi = multi
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False
        self.bytes_written = 0
        self.done_callback = done_callback

    def start(self):
        self.multi.add_fetch(self, self.urls[0])

    def success(self):
        self.has_completed = True
        self.has_succeeded = True
        self.done_callback()

    def failure(self, errno, errmsg):
        self.failures += 1
        self.reset()
        try:
            self.multi.add_fetch(self, self.urls[self.failures])
        except IndexError:
            cherrypy.log.error('No more URLs to try.', 'BLOCKSTORE', logging.ERROR)
            self.has_completed = True
            self.has_succeeded = False
            self.done_callback()

    def reset(self):
        pass

class FileTransferContext(RetryTransferContext):

    def __init__(self, urls, save_id, multi, callback):
        RetryTransferContext.__init__(self, urls, multi, callback)
        with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
            self.sinkfile_name = sinkfile.name
        self.sink_fp = open(self.sinkfile_name, "wb")
        self.save_id = save_id

    def write_data(self, _str):
        cherrypy.log.error("Fetching file syncly %s writing %d bytes" % 
                           (self.save_id, len(_str)), 'CURL_FETCH', logging.DEBUG)
        self.bytes_written += len(_str)
        ret = self.sink_fp.write(_str)
        cherrypy.log.error("Now at position %d (result of write was %s)" % 
                           (self.sink_fp.tell(), str(ret)), 'CURL_FETCH', logging.DEBUG)

    def reset(self):
        cherrypy.log.error('Failed to fetch %s from %s, retrying...' % 
                           (self.save_id, self.urls[self.failures-1]), 
                           'BLOCKSTORE', logging.WARNING)
        self.sink_fp.seek(0)
        self.sink_fp.truncate(0)

    def cleanup(self):
        cherrypy.log.error('Closing sink file for %s (wrote %d bytes, tell = %d, errors = %s)' % 
                           (self.save_id, self.bytes_written, self.sink_fp.tell(), str(self.sink_fp.errors)), 
                           'CURL_FETCH', logging.DEBUG)
        self.sink_fp.close()
        cherrypy.log.error('File now closed (errors = %s)' % 
                           self.sink_fp.errors, 'CURL_FETCH', logging.DEBUG)

    def save_result(self, block_store):
        if self.has_completed and self.has_succeeded:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class BufferTransferContext(RetryTransferContext):

    def __init__(self, urls, multi, callback):
        RetryTransferContext.__init__(self, urls, multi, callback)
        self.buffer = StringIO()

    def write_data(self, _str):
        self.buffer.write(_str)

    def reset(self):
        self.buffer.close()
        self.buffer = StringIO()

    def cleanup(self):
        self.buffer.close()

class WaitableTransferGroup:

    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.transfers_done = 0

    def transfer_completed_callback(self):
        with self._lock:
            self.transfers_done += 1
            self._cond.notify_all()

    def wait_for_transfers(self, n):
        with self._lock:
            while self.transfers_done < n:
                self._cond.wait()

class StreamTransferGroup(WaitableTransferGroup):
    def __init__(self):
        WaitableTransferGroup.__init__(self)
        self.handles = []
        self.handles_lock = threading.Lock()

    def add_handle(self, h):
        with self.handles_lock:
            self.handles.append(h)

    def notify_streams_done(self):
        # Out-of-thread call. Might overlap with any other member functions.
        # I think this is enough; consumer_attached/detached can overlap freely,
        # as they're synchronised by post_event, whilst cleanup only runs once the
        # transfer is done, which the event handler checks for.
        with self.handles_lock:
            for h in self.handles:
                h.try_again_now()

    def wait_for_all_transfers(self):
        self.wait_for_transfers(len(self.handles))

    def consumers_attached(self):
        for h in self.handles:
            h.consumer_attached()

    def consumers_detached(self):
        for h in self.handles:
            h.consumer_detached()

    def log_traces(self):
        for h in self.handles:
            h.log_trace()

    def cleanup(self, block_store):
        for handle in self.handles:
            # Cleanup must happen first, because it typically closes the file that we are storing in the block store.
            handle.cleanup()
            handle.save_result(block_store)
        
    def get_failed_refs(self):
        failure_bindings = {}
        for handle in self.handles:
            if not handle.has_succeeded:
                failure_bindings[handle.ref.id] = SW2_TombstoneReference(handle.ref.id, handle.ref.location_hints)
        if len(failure_bindings) > 0:
            return failure_bindings
        else:
            return None

FIFO_UNCONNECTED = 0
FIFO_CONNECTED = 1
FIFO_DEAD = 2

class StreamTransferContext(pycURLContextCallbacks):
    # Represents a complete stream attempt which might involve many fetches

    class StreamFetchContext(pycURLFetchCallbacks):
        # Represents a single HTTP-fetch attempt in stream

        def __init__(self, ctx, description, range):
            self.ctx = ctx
            self.response_had_stream = False
            self.request_length = None
            self.first_header = True
            self.response_code = None
            self.description = description
            self.range = range
        
        def write_data(self, _str):
            # Don't write error-documents to our client process
            if self.response_code is not None and self.response_code < 300:
                self.ctx.write_data(_str)

        def write_header_line(self, _str):
            if self.first_header:
                self.first_header = False
                match_obj = http_response_regex.match(_str)
                if match_obj is not None:
                    self.response_code = int(match_obj.group(1))
            if _str.startswith("Pragma") != -1 and _str.find("streaming") != -1:
                self.response_had_stream = True

            # XXX: If response does not contain a Content-Length header, 
            #      self.request_length will be set to None.
            match_obj = length_regex.match(_str)
            if match_obj is not None:
                self.request_length = int(match_obj.group(1))

        def success(self):
            self.ctx.request_succeeded()

        def failure(self, errno, errstr):
            self.ctx.request_failed(errno, errstr)

    class StreamFifoSink:
        def __init__(self, fifo_name, max_buffer_level, report_index, do_log=False):
            self.dummy_read_fd = os.open(fifo_name, os.O_RDONLY | os.O_NONBLOCK)
            self.fifo_fd = os.open(fifo_name, os.O_WRONLY | os.O_NONBLOCK)
            self.fifo_state = FIFO_UNCONNECTED
            self.mem_buffer = ""
            self.buffer_limit = max_buffer_level
            self.eof = False
            self.report_index = report_index
            if do_log:
                self.io_trace = []
            else:
                self.io_trace = None

        def trace_event(self, str):
            if self.io_trace is not None:
                self.io_trace.append((time.time(), str))

        def log_trace(self):
            for (t, e) in self.io_trace:
                cherrypy.engine.publish("worker_event", "Fetch FIFO trace %d %f %s" % (self.report_index, t, e))

        def write_without_blocking(self, fd, _str):
            total_written = 0
            while len(_str) > 0:
                try:
                    bytes_written = os.write(self.fifo_fd, _str)
                    _str = _str[bytes_written:]
                    total_written += bytes_written
                except OSError, e:
                    if e.errno == EAGAIN:
                        return total_written
                    else:
                        raise
            return total_written

        def buffer_empty(self):
            return (len(self.mem_buffer) == 0)

        def try_empty_buffer(self):
            assert self.fifo_state == FIFO_CONNECTED
            if not self.buffer_empty():
                try:
                    written = self.write_without_blocking(self.fifo_fd, self.mem_buffer)
                    self.mem_buffer = self.mem_buffer[written:]
                except OSError, e:
                    if e.errno == EPIPE:
                        self.consumer_detached()
                    else:
                        raise
            else:
                self.trace_event("Buffer empty")
            if self.buffer_empty() and self.eof:
                self.consumer_detached()

        
        def write_data(self, _str):
            if self.fifo_state == FIFO_DEAD:
                return
            elif self.fifo_state == FIFO_UNCONNECTED:
                self.mem_buffer += _str
            elif self.fifo_state == FIFO_CONNECTED:
                self.try_empty_buffer()
                if self.fifo_state == FIFO_CONNECTED:
                    if not self.buffer_empty():
                        self.mem_buffer += _str
                    else:
                        written = self.write_without_blocking(self.fifo_fd, _str)
                        if written < len(_str):
                            self.trace_event("Buffer not empty")
                            self.mem_buffer += _str[written:]

        def write_data_eof(self):
            # If the FIFO is still unconnected we should wait for it to become connected.
            # If it's connected already, just hang up.
            self.eof = True
            self.trace_event("EOF")
            if self.fifo_state == FIFO_CONNECTED:
                if self.buffer_empty():
                    self.consumer_detached()
                # Otherwise this will happen in try_empty_buffer
            elif self.fifo_state == FIFO_DEAD:
                return
            elif self.fifo_state == FIFO_UNCONNECTED:
                # EOF will be delivered in consumer_attached, by way of try_empty_buffer.
                return

        def select_fds(self):
            if self.fifo_state == FIFO_CONNECTED and not self.buffer_empty():
                return [], [self.fifo_fd], []
            else:
                return [], [], []

        def select_callback(self, rd, wr, exn):
            if self.fifo_state == FIFO_CONNECTED and self.fifo_fd in wr:
                self.try_empty_buffer()

        def max_allowable_fetch(self):
            if self.fifo_state == FIFO_UNCONNECTED or self.fifo_state == FIFO_CONNECTED:
                return self.buffer_limit - len(self.mem_buffer)
            else:
                return None

        def consumer_attached(self):
            assert self.fifo_state == FIFO_UNCONNECTED
            self.trace_event("Became attached")
            self.fifo_state = FIFO_CONNECTED
            os.close(self.dummy_read_fd)
            self.dummy_read_fd = -1
            self.try_empty_buffer()

        def consumer_detached(self):
            self.trace_event("Became detached")
            if self.fifo_state == FIFO_DEAD:
                return
            elif self.fifo_state == FIFO_UNCONNECTED:
                os.close(self.fifo_fd)
                os.close(self.dummy_read_fd)
                self.fifo_fd = -1
                self.dummy_read_fd = -1
                self.mem_buffer = ""
            elif self.fifo_state == FIFO_CONNECTED:
                os.close(self.fifo_fd)
                self.fifo_fd = -1
                self.mem_buffer = ""
            self.fifo_state = FIFO_DEAD
            
    def __init__(self, ref, urls, save_id, multi, completion_callback, report_index, do_io_trace=False):
        self.ref = ref
        self.urls = urls
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False

        # Hard-coded stream behaviour
        self.max_in_memory_buffer = 1048576
        self.min_fetch_size = 262144

        self.fifo_dir = tempfile.mkdtemp()
        self.fifo_name = os.path.join(self.fifo_dir, 'fetch_fifo')
        os.mkfifo(self.fifo_name)
        self.fifo_sink = StreamTransferContext.StreamFifoSink(self.fifo_name, self.max_in_memory_buffer, report_index, do_io_trace)
        with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
            self.sinkfile_name = sinkfile.name
        self.sink_fp = open(self.sinkfile_name, "wb")
        self.current_start_byte = 0

        self.have_written_to_process = False
        self.current_fetch = None
        self.dormant_until = None
        self.requests_paused = False
        self.event_queue = SelectableEventQueue()

        self.save_id = save_id
        self.multi = multi
        self.description = self.urls[0]
        self.completion_callback = completion_callback
        self.report_index = report_index
        if do_io_trace:
            cherrypy.log.error("DEBUG: pycURL trace active", "CURL_FETCH", logging.INFO)
            self.io_trace = []
        else:
            self.io_trace = None
        multi.add_context(self)

    def trace_event(self, str):
        if self.io_trace is not None:
            self.io_trace.append((time.time(), str))

    def log_trace(self):
        for (t, e) in self.io_trace:
            cherrypy.engine.publish("worker_event", "Fetch trace %d %f %s" % (self.report_index, t, e))
        self.fifo_sink.log_trace()

    def start(self):
        self.trace_event("Start")
        self.start_next_fetch()

    def get_select_fds(self):
        # Called from cURL thread
        read_fds, write_fds, exn_fds = self.fifo_sink.select_fds()
        ev_rfds, ev_wfds, ev_exfds = self.event_queue.get_select_fds()
        read_fds.extend(ev_rfds)
        write_fds.extend(ev_wfds)
        exn_fds.extend(ev_exfds)
        return read_fds, write_fds, exn_fds

    def get_timeout(self):
        # Called from cURL thread
        return self.dormant_until

    def consider_restart(self):
        if self.dormant_until is None and not self.requests_paused:
            cherrypy.log.error("Restart %s fetch" % self.description,
                               'CURL_FETCH', logging.DEBUG)
            self.start_next_fetch()

    def fifo_overfull(self):
        max_fetch = self.fifo_sink.max_allowable_fetch()
        if max_fetch is None:
            return False
        else:
            return (max_fetch < self.min_fetch_size)

    def select_callback(self, rd, wr, exn):
        # Called from cURL thread
        # Reasons this callback could occur:
        # 1. The FIFO is writable
        # 2. It's time to retry a paused request.
        # 3. An out-of-thread entity has pushed events into our event queue.
        self.event_queue.dispatch_events()
        self.fifo_sink.select_callback(rd, wr, exn)
        if self.requests_paused:
            if not self.fifo_overfull():
                self.requests_paused = False
                self.consider_restart()
        if self.dormant_until is not None:
            if datetime.now() > self.dormant_until:
                self.dormant_until = None
                self.consider_restart()

    def write_data(self, _str):
        # Called from cURL thread
        self.trace_event("receiving")
        self.fifo_sink.write_data(_str)
        ret = self.sink_fp.write(_str)
        self.current_start_byte += len(_str)
        self.have_written_to_process = True

    def start_fetch(self, url, range=None):
        self.current_fetch = StreamTransferContext.StreamFetchContext(self, url, range)
        self.multi.add_fetch(self.current_fetch, url, range)

    def start_next_fetch(self):
        fifo_fetch_limit = self.fifo_sink.max_allowable_fetch()
        if fifo_fetch_limit is None:
            cherrypy.log.error("Unbounded fetch %s offset %d" %
                               (self.description, self.current_start_byte))
            fetch_end = None
        else:
            cherrypy.log.error("Bounded fetch %s offset %d length %d" % 
                               (self.description, self.current_start_byte, fifo_fetch_limit), 'CURL_FETCH', logging.DEBUG)
            fetch_end = self.current_start_byte + fifo_fetch_limit
        self.trace_event("Request-sent")
        self.start_fetch(self.urls[self.failures], (self.current_start_byte, fetch_end))

    def pause_for(self, secs):
        assert self.dormant_until is None
        self.trace_event("Dormant")
        self.dormant_until = datetime.now() + timedelta(0, secs)
        cherrypy.log.error("Pausing %s fetch due to producer buffer empty" 
                           % self.description, 'CURL_FETCH', logging.DEBUG)

    def request_succeeded(self):
        # Called from cURL thread
        (req_start, req_end) = self.current_fetch.range
        if req_end is None:
            req_bytes = None
        else:
            req_bytes = (req_end - req_start) + 1
        rx_length = self.current_fetch.request_length
        if req_bytes is None or rx_length is None or req_bytes > rx_length:
            # Potentially the end
            if not self.current_fetch.response_had_stream:
                self.report_completion(True)
        if not self.has_completed:
            if not self.fifo_overfull():
                if rx_length < self.min_fetch_size:
                    # We're nearly ahead of the producer; pause to give it a chance to buffer more output.
                    self.pause_for(1)
                else:
                    self.start_next_fetch()
            else:
                # We should hold off on ordering another chunk until the process has consumed this one
                # The restart will happen when the buffer drains.
                cherrypy.log.error("Pausing %s fetch consumer buffer overfull" % self.description, 
                                   'CURL_FETCH', logging.DEBUG)
                self.requests_paused = True

    def report_completion(self, succeeded):
        self.trace_event("EOF")
        cherrypy.log.error("%s reporting EOF to client" % self.description, "CURL_FETCH", logging.INFO)
        self.fifo_sink.write_data_eof()
        self.has_completed = True
        self.has_succeeded = succeeded
        self.completion_callback()

    def request_failed(self, errno, errmsg):
        # Called from cURL thread
        if errno == 418:
            if not self.current_fetch.response_had_stream:
                self.report_completion(True)
            else:
                # We've got ahead of the producer.
                # Pause for 1 seconds.
                self.pause_for(1)

        else:
            cherrypy.log.error("Fetch %s failed (error %s)" % 
                               (self.description, str(errno)), 
                               "CURL_FETCH", logging.WARNING)
            if self.have_written_to_process:
                # Can't just fail-over; we've failed after having written bytes to a process.
                self.report_completion(False)
            else:
                self.failures += 1
                try:
                    self.start_fetch(self.urls[self.failures], (0, self.chunk_size))
                except IndexError:
                    # Run out of URLs to try
                    self.report_completion(False)

    def _consumer_attached(self):
        # Called from cURL thread
        cherrypy.log.error("Client process for %s attached" % self.description, "CURL_FETCH", logging.INFO)
        self.fifo_sink.consumer_attached()

    def consumer_attached(self):
        # Called from arbitrary thread
        self.event_queue.post_event(self._consumer_attached)

    def _consumer_detached(self):
        # Called from cURL thread
        cherrypy.log.error("Client process for %s detached" % self.description, "CURL_FETCH", logging.INFO)
        self.fifo_sink.consumer_detached()
        if self.requests_paused and not self.has_completed:
            self.requests_paused = False
            self.consider_restart()

    def consumer_detached(self):
        # Called from arbitrary thread
        self.event_queue.post_event(self._consumer_detached)

    def _try_again_now(self):
        # Called from cURL thread
        if not self.has_completed:
            cherrypy.log.error("Transfer %s trying again immediately (producer reported done via master)" % self.description, "CURL_FETCH", logging.INFO)
            if self.dormant_until is not None:
                self.dormant_until = None
                self.consider_restart()

    def try_again_now(self):
        # Called from arbitrary thread
        self.event_queue.post_event(self._try_again_now)
        
    def cleanup(self):
        # Called from arbitrary thread, but only after all cURL callbacks have completed
        cherrypy.log.error('Closing sink file for %s (wrote %d bytes, tell = %d, errors = %s)' 
                           % (self.save_id, self.current_start_byte, self.sink_fp.tell(), str(self.sink_fp.errors)), 
                           'CURL_FETCH', logging.DEBUG)
        self.sink_fp.flush()
        self.sink_fp.close()
        cherrypy.log.error('File now closed (errors = %s)' % self.sink_fp.errors, 'CURL_FETCH', logging.DEBUG)
        os.unlink(self.fifo_name)
        os.rmdir(self.fifo_dir)
        self.multi.remove_context(self)
        self.event_queue.cleanup()

    def save_result(self, block_store):
        # Called from arbitrary thread, but only after all cURL callbacks have completed
        if self.has_completed and self.has_succeeded:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class BlockStore(plugins.SimplePlugin):

    def __init__(self, bus, hostname, port, base_dir, ignore_blocks=False):
        plugins.SimplePlugin.__init__(self, bus)
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
        self.bus = bus
        self.bus.subscribe("stop", self.stop_thread, 10)
        self.fetch_thread = pycURLThread()
        self.fetch_thread.start()
    
        self.pin_set = set()
    
        self.ignore_blocks = ignore_blocks
    
        # Maintains a set of block IDs that are currently being written.
        # (i.e. They are in the pre-publish/streamable state, and have not been
        #       committed to the block store.)
        self.streaming_id_set = set()
    
        self.current_cache_access_id = 0
        self.url_cache_filenames = {}
        self.url_cache_access_times = {}
        
        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle, 'handle': self.decode_handle, 'script': self.decode_script}

    def stop_thread(self):
        self.fetch_thread.stop()

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
        
    def mark_url_as_accessed(self, url):
        self.url_cache_access_times[url] = self.current_cache_access_id
        self.current_cache_access_id += 1
        
    def find_url_in_cache(self, url):
        with self._lock:
            try:
                ret = self.url_cache_filenames[url]
            except KeyError:
                return None
            self.mark_url_as_accessed(url)
            return ret
    
    def evict_lru_from_url_cache(self):
        lru_url = min([(access_time, url) for (url, access_time) in self.url_cache_access_times.items()])[1]
        del self.url_cache_filenames[lru_url]
        del self.url_cache_access_times[lru_url] 
    
    def allocate_new_id(self):
        return str(uuid.uuid1())
    
    CACHE_SIZE_LIMIT=1024
    def store_url_in_cache(self, url, filename):
        with self._lock:
            if len(self.url_cache_filenames) == BlockStore.CACHE_SIZE_LIMIT:
                self.evict_lru_from_url_cache()
            self.url_cache_filenames[url] = filename
            self.mark_url_as_accessed(url)
    
    def maybe_streaming_filename(self, id):
        with self._lock:
            if id in self.streaming_id_set:
                return True, self.streaming_filename(id)
            else:
                return False, self.filename(id)
    
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
    
    def store_object(self, object, encoder, id):
        """Stores the given object as a block, and returns a swbs URL to it."""
        #self.object_cache[id] = object
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

    def make_stream_sink(self, id):
        '''
        Called when an executor wants its output to be streamable.
        This method only prepares the block store, and
        '''
        with self._lock:
            self.streaming_id_set.add(id)
            filename = self.streaming_filename(id)
            open(filename, 'wb').close()
            return filename

    def prepublish_file(self, filename, id):
        '''
        Called when an executor wants its output to be streamable.
        This method only prepares the block store, and
        '''
        cherrypy.log.error('Prepublishing file %s for output %s' % (filename, id), 'BLOCKSTORE', logging.INFO)
        with self._lock:
            self.streaming_id_set.add(id)
            os.symlink(filename, self.streaming_filename(id))
                   
    def commit_file(self, filename, id, can_move=False):
        cherrypy.log.error('Committing streamed file %s for output %s' % (filename, id), 'BLOCKSTORE', logging.INFO)
        if can_move:
            # Moving the file under the lock should be cheap.
            # N.B. We need to protect all operations because the streaming
            #      filename will be unlinked.
            with self._lock:
                url, file_size = self.store_file(filename, id, True)
                self.streaming_id_set.remove(id)
                os.unlink(self.streaming_filename(id))
                os.symlink(self.filename(id), self.streaming_filename(id))
        else:
            # A copy will be necessary, so do this outside the lock.
            url, file_size = self.store_file(filename, id, False)
            with self._lock:
                self.streaming_id_set.remove(id)
                os.unlink(self.streaming_filename(id))
                os.symlink(self.filename(id), self.streaming_filename(id))
            
        return url, file_size

    def rollback_file(self, id):
        cherrypy.log.error('Rolling back streamed file for output %s' % id, 'BLOCKSTORE', logging.WARNING)
        with self._lock:
            self.streaming_id_set.remove(id)
            os.unlink(self.streaming_filename(id))
    
    def try_retrieve_filename_for_ref_without_transfer(self, ref):
        assert isinstance(ref, SWRealReference)

        def find_first_cached(urls):
            for url in urls:
                filename = self.find_url_in_cache(url)
                if filename is not None:
                    return filename
            return None

        if isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()
        elif isinstance(ref, SWDataValue):
            id = ref.id
            with open(self.filename(id), 'w') as obj_file:
                self.encode_json(ref.value, obj_file)
            return self.filename(id)
        elif isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference):
            maybe_local_filename = self.filename(ref.id)
            if os.path.exists(maybe_local_filename):
                return maybe_local_filename
            check_urls = ["swbs://%s/%s" % (loc_hint, str(ref.id)) for loc_hint in ref.location_hints]
            return find_first_cached(check_urls)
        elif isinstance(ref, SWURLReference):
            for url in ref.urls:
                parsed_url = urlparse.urlparse(url)
                if parsed_url.scheme == "file":
                    return parsed_url.path
            return find_first_cached(ref.urls)

    def try_retrieve_object_for_ref_without_transfer(self, ref, decoder):

        # Basically like the same task for files, but with the possibility of cached decoded forms
        if isinstance(ref, SW2_ConcreteReference):
            for loc in ref.location_hints:
                if loc == self.netloc:
                    try:
                        return self.object_cache[ref.id]
                    except:
                        pass
        cached_file = self.try_retrieve_filename_for_ref_without_transfer(ref)
        if cached_file is not None:
            with open(cached_file, "r") as f:
                return self.decoders[decoder](f)
        return None

    def get_fetch_urls_for_ref(self, ref):

        if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference):
            return ["http://%s/data/%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
        elif isinstance(ref, SWURLReference):
            return map(sw_to_external_url, ref.urls)
        elif isinstance(ref, SW2_FetchReference):
            return [ref.url]
                
    def retrieve_filenames_for_refs_eager(self, refs):

        fetch_ctx = WaitableTransferGroup()

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference) or isinstance(ref, SW2_FetchReference):
                save_id = ref.id
            else:
                save_id = self.allocate_new_id()
            new_ctx = FileTransferContext(urls, save_id, self.fetch_thread, fetch_ctx.transfer_completed_callback)
            new_ctx.start()
            return new_ctx

        request_list = []
        for (ref, resolution) in zip(refs, resolved_refs):
            if resolution is None:
                request_list.append(create_transfer_context(ref))

        fetch_ctx.wait_for_transfers(len(request_list))

        for req in request_list:
            req.save_result(self)

        failure_bindings = {}
        for req in request_list:
            if not req.has_succeeded:
                failure_bindings[req.ref.id] = SW2_TombstoneReference(req.ref.id, req.ref.location_hints)
        if len(failure_bindings) > 0:
            raise MissingInputException(failure_bindings)

        result_list = []
 
        for resolution in resolved_refs:
            if resolution is None:
                next_req = request_list.pop(0)
                next_req.cleanup()
                result_list.append(self.filename(next_req.save_id))
            else:
                result_list.append(resolution)

        return result_list

    def retrieve_filenames_for_refs(self, refs, do_io_trace=False):

        fetch_ctx = StreamTransferGroup()

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref, i):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference):
                save_id = ref.id
            else:
                save_id = self.allocate_new_id()
            ret = StreamTransferContext(ref, urls, save_id, self.fetch_thread, 
                                        fetch_ctx.transfer_completed_callback, i, do_io_trace)
            fetch_ctx.add_handle(ret)
            ret.start()
            return ret

        result_list = []
 
        for (i, (ref, resolution)) in enumerate(zip(refs, resolved_refs)):
            if resolution is None:
                ctx = create_transfer_context(ref, i)
                result_list.append(ctx.fifo_name)
            else:
                result_list.append(resolution)

        return (result_list, fetch_ctx)

    def retrieve_objects_for_refs(self, refs, decoder):
        
        easy_solutions = [self.try_retrieve_object_for_ref_without_transfer(ref, decoder) for ref in refs]
        fetch_urls = [self.get_fetch_urls_for_ref(ref) for ref in refs]

        result_list = []
        request_list = []
        
        transfer_ctx = WaitableTransferGroup()

        for (solution, this_fetch_urls, ref) in zip(easy_solutions, fetch_urls, refs):
            if solution is not None:
                result_list.append(solution)
            else:
                result_list.append(None)
                new_ctx = BufferTransferContext(this_fetch_urls, 
                                                self.fetch_thread, 
                                                transfer_ctx.transfer_completed_callback)
                new_ctx.start()
                request_list.append((ref, new_ctx))

        transfer_ctx.wait_for_transfers(len(request_list))

        failure_bindings = {}
        j = 0
        for i, res in enumerate(result_list):
            if res is None:
                ref, next_object = request_list[j]
                j += 1
                if next_object.has_succeeded:
                    next_object.buffer.seek(0)
                    result_list[i] = self.decoders[decoder](next_object.buffer)
                else:
                    failure_bindings[ref.id] = SW2_TombstoneReference(ref.id, ref.location_hints)

        for _, req in request_list:
            req.cleanup()
        
        if len(failure_bindings) > 0:
            raise MissingInputException(failure_bindings)
        else:
            return result_list

    def retrieve_object_for_ref(self, ref, decoder):
        return self.retrieve_objects_for_refs([ref], decoder)[0]
        
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
        cherrypy.log.error('Generating block list for local consumption', 'BLOCKSTORE', logging.INFO)
        for block_name in os.listdir(self.base_dir):
            if not block_name.startswith('.'):
                block_size = os.path.getsize(os.path.join(self.base_dir, block_name))
                yield block_name, block_size
    
    def build_pin_set(self):
        cherrypy.log.error('Building pin set', 'BLOCKSTORE', logging.INFO)
        initial_size = len(self.pin_set)
        for filename in os.listdir(self.base_dir):
            if filename.startswith(PIN_PREFIX):
                self.pin_set.add(filename[len(PIN_PREFIX):])
                cherrypy.log.error('Pinning block %s' % filename[len(PIN_PREFIX):], 'BLOCKSTORE', logging.INFO)
        cherrypy.log.error('Pinned %d new blocks' % (len(self.pin_set) - initial_size), 'BLOCKSTORE', logging.INFO)
    
    def generate_block_list_file(self):
        cherrypy.log.error('Generating block list file', 'BLOCKSTORE', logging.INFO)
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
        cherrypy.log.error('Pinned block %s' % id, 'BLOCKSTORE', logging.INFO)
        
    def flush_unpinned_blocks(self, really=True):
        cherrypy.log.error('Flushing unpinned blocks', 'BLOCKSTORE', logging.INFO)
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
            cherrypy.log.error('Flushed block store, kept %d blocks, removed %d blocks' % (files_kept, files_removed), 'BLOCKSTORE', logging.INFO)
        else:
            cherrypy.log.error('If we flushed block store, would keep %d blocks, remove %d blocks' % (files_kept, files_removed), 'BLOCKSTORE', logging.INFO)
        return (files_kept, files_removed)

    def is_empty(self):
        return self.ignore_blocks or len(os.listdir(self.base_dir)) == 0
