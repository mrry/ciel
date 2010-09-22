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
from datetime import datetime, timedelta
from cStringIO import StringIO
from errno import EAGAIN

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from skywriting.runtime.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SWNullReference, SWURLReference, \
    SWTaskOutputProvenance, SW2_StreamReference,\
    SW2_TombstoneReference, SW2_FetchReference, SWNoProvenance
import hashlib
import contextlib
from skywriting.lang.parser import CloudScriptParser
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

PIN_PREFIX = '.__pin__:'

length_regex = re.compile("^Content-Length:\s*([0-9]+)")

class StreamRetry:
    pass
STREAM_RETRY = StreamRetry()

def get_netloc_for_sw_url(url):
    return urlparse.urlparse(url).netloc

def get_id_for_sw_url(url):
    return urlparse.urlparse(url).path

class SWReferenceJSONEncoder(simplejson.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, SWRealReference):
            return {'__ref__': obj.as_tuple()}
        else:
            return simplejson.JSONEncoder.default(self, obj)

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

class TransferSetContext:
    def __init__(self):
        self.curl_ctx = pycurl.CurlMulti()
        self.curl_ctx.setopt(pycurl.M_PIPELINING, 0)
        self.curl_ctx.setopt(pycurl.M_MAXCONNECTS, 20)
        self.active_handles = 0
        self._handles = []

    def start_fetch(self, ctx):
        self.active_handles += 1
        self.curl_ctx.add_handle(ctx.curl_ctx)
        self._handles.append(ctx)

    def process(self):

        while 1:
            go_again = False
            while 1:
                # Will generate write_data callbacks as appropriate
                ret, num_handles = self.curl_ctx.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            while 1:
                num_q, ok_list, err_list = self.curl_ctx.info_read()
                if len(ok_list) != 0 or len(err_list) != 0:
                    # We'll be making callbacks, which might schedule more work.
                    # We should try processing again afterwards;
                    # worst that happens, perform() will return OK right away and we'll drop out.
                    go_again = True
                for c in ok_list:
                    self.curl_ctx.remove_handle(c)
                    self.active_handles -= 1
                    c.ctx._success()
                for c, errno, errmsg in err_list:
                    self.curl_ctx.remove_handle(c)
                    self.active_handles -= 1
                    cherrypy.log.error("Curl failure: %s, %s" % (str(errno), str(errmsg)), "CURL_FETCH", logging.WARNING)
                    c.ctx._failure(errno, errmsg)
                if num_q == 0:
                    break
            if not go_again:
                break

    def transfer_all(self):
        while self.active_handles > 0:
            self.process()
            self.curl_ctx.select(5.0)

    def cleanup(self):
        self.curl_ctx.close()

    def get_failed_refs(self):
        failure_bindings = {}
        for handle in self._handles:
            if not handle.has_succeeded:
                failure_bindings[handle.ref.id] = SW2_TombstoneReference(handle.ref.id, handle.ref.location_hints)
        if len(failure_bindings) > 0:
            return failure_bindings
        else:
            return None

class StreamTransferSetContext(TransferSetContext):
    def __init__(self):
        TransferSetContext.__init__(self)
        self.handles = []

    def writable_handles(self):
        return filter(lambda x: (not x.has_completed) and len(x.mem_buffer) > 0, self.handles)

    def write_waitable_handles(self):
        return filter(lambda x: (not x.has_completed) and len(x.mem_buffer) > 0 and x.fifo_fd != -1, self.handles)

    def dormant_handles(self):
        return filter(lambda x: x.dormant_until is not None, self.handles)

    def process(self):
        while 1:
            go_again = False
            # go_again is set when cURL's handle-group might have changed, since the last perform(),
            # so it's not safe to select() at this time.
            TransferSetContext.process(self)
            for handle in self.writable_handles():
                if handle.try_empty_buffer():
                    go_again = True
            for handle in self.dormant_handles():
                if datetime.now() > handle.dormant_until:
                    handle.wake_up()
                    go_again = True
            if not go_again:
                return

    def drain_pipe(self, pipefd):
        oldflags = fcntl.fcntl(pipefd, fcntl.F_GETFL)
        newflags = oldflags | os.O_NONBLOCK
        fcntl.fcntl(pipefd, fcntl.F_SETFL, newflags)
        try:
            while(os.read(pipefd, 1024) >= 0):
                pass
        except OSError, e:
            if e.errno == EAGAIN:
                return
            else:
                raise

    def select(self, death_pipe):

        shortest_timeout = 15.0

        def td_secs(td):
            return (float(td.microseconds) / 10**6) + float(td.seconds)

        for handle in self.dormant_handles():
            time_to_wait = td_secs(handle.dormant_until - datetime.now())
            if time_to_wait < shortest_timeout:
                shortest_timeout = time_to_wait
        (read, write, exn) = self.curl_ctx.fdset()
        for handle in self.write_waitable_handles():
            write.append(handle.fifo_fd)
        read.append(death_pipe)
        read_ret, write_ret, exn_ret = select.select(read, write, exn, shortest_timeout)
        if death_pipe in read_ret:
            self.drain_pipe(death_pipe)
            cherrypy.log.error("Closing fetch FIFOs due to process death", 'CURL_FETCH', logging.DEBUG)
            for handle in self.handles:
                handle.fifo_closed()

    def transfer_all(self, death_pipe):
        while 1:
            self.process()
            remaining_transfers = filter(lambda x: not x.has_completed, self.handles)
            if len(remaining_transfers) == 0:
                cherrypy.log.error("All transfers complete", 'CURL_FETCH', logging.DEBUG)
                return
            self.select(death_pipe)

    def cleanup(self, block_store):
        for handle in self.handles:
            handle.save_result(block_store)
            handle.cleanup()
        TransferSetContext.cleanup(self)
        
    def get_failed_refs(self):
        failure_bindings = {}
        for handle in self.handles:
            if not handle.has_succeeded:
                failure_bindings[handle.ref.id] = SW2_TombstoneReference(handle.ref.id, handle.ref.location_hints)
        if len(failure_bindings) > 0:
            return failure_bindings
        else:
            return None

class TransferContext:

    def __init__(self, multi):

        self.multi = multi
        self.curl_ctx = pycurl.Curl()
        self.curl_ctx.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl_ctx.setopt(pycurl.MAXREDIRS, 5)
        self.curl_ctx.setopt(pycurl.CONNECTTIMEOUT, 30)
        self.curl_ctx.setopt(pycurl.TIMEOUT, 300)
        self.curl_ctx.setopt(pycurl.NOSIGNAL, 1)
        self.curl_ctx.setopt(pycurl.WRITEFUNCTION, self.write_data)
        self.curl_ctx.setopt(pycurl.HEADERFUNCTION, self.write_header_line)
        self.curl_ctx.ctx = self
        self.active = False

    def start_fetch(self, url, range=None):
        if self.active:
            raise Exception("Bad state: tried to start_fetch a curl context which was already active")
        self.active = True
        self.curl_ctx.setopt(pycurl.URL, str(url))
        if range is not None:
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-%d" % range])
        self.multi.start_fetch(self)
            
    def _success(self):
        self.active = False
        response_code = self.curl_ctx.getinfo(pycurl.RESPONSE_CODE)
        if str(response_code).startswith("2"):
            self.success()
        else:
            self.failure(response_code, "")

    def _failure(self, errno, errmsg):
        self.active = False
        self.failure(errno, errmsg)

    def cleanup(self):
        self.curl_ctx.close()

class BufferTransferContext(TransferContext):

    def __init__(self, urls, multi):
        TransferContext.__init__(self, multi)
        self.buffer = StringIO()
        self.urls = urls
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False
        self.start_fetch(self.urls[0])

    def write_data(self, _str):
        self.buffer.write(_str)

    def write_header_line(self, _str):
        pass

    def success(self):
        self.has_completed = True
        self.has_succeeded = True

    def failure(self, errno, errmsg):
        self.failures += 1
        try:
            cherrypy.log.error('Failed to fetch from %s (%s, %s), retrying...' % (self.urls[self.failures-1], str(errno), errmsg), 'BLOCKSTORE', logging.WARNING)
            self.start_fetch(self.urls[self.failures])
            self.buffer.close()
            self.buffer = StringIO()
        except IndexError:
            cherrypy.log.error('No more URLs to try.', 'BLOCKSTORE', logging.ERROR)
            self.has_completed = True
            self.has_succeeded = False

    def cleanup(self):
        self.buffer.close()
        TransferContext.cleanup(self)

class FileTransferContext(TransferContext):

    def __init__(self, urls, save_id, multi):
        TransferContext.__init__(self, multi)
        with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
            self.sinkfile_name = sinkfile.name
        self.sink_fp = open(self.sinkfile_name, "wb")
        self.urls = urls
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False
        self.save_id = save_id
        self.start_fetch(self.urls[0])
        self.bytes_written = 0

    def write_data(self, _str):
        cherrypy.log.error("Fetching file syncly %s writing %d bytes" % (self.save_id, len(_str)), 'CURL_FETCH', logging.DEBUG)
        self.bytes_written += len(_str)
        ret = self.sink_fp.write(_str)
        cherrypy.log.error("Now at position %d (result of write was %s)" % (self.sink_fp.tell(), str(ret)), 'CURL_FETCH', logging.DEBUG)

    def write_header_line(self, _str):
        pass

    def success(self):
        self.has_completed = True
        self.has_succeeded = True

    def failure(self, errno, errmsg):
        self.failures += 1
        try:
            cherrypy.log.error('Failed to fetch %s from %s (%s, %s), retrying...' % (self.save_id, self.urls[self.failures-1], str(errno), errmsg), 'BLOCKSTORE', logging.WARNING)
            self.sink_fp.seek(0)
            self.sink_fp.truncate(0)
            self.start_fetch(self.urls[self.failures])
        except IndexError:
            cherrypy.log.error('No more URLs to try.', 'BLOCKSTORE', logging.ERROR)
            self.has_completed = True
            self.has_succeeded = False

    def cleanup(self):
        cherrypy.log.error('Closing sink file for %s (wrote %d bytes, tell = %d, errors = %s)' % (self.save_id, self.bytes_written, self.sink_fp.tell(), str(self.sink_fp.errors)), 'CURL_FETCH', logging.DEBUG)
        self.sink_fp.close()
        cherrypy.log.error('File now closed (errors = %s)' % self.sink_fp.errors, 'CURL_FETCH', logging.DEBUG)
        TransferContext.cleanup(self)

    def save_result(self, block_store):
        if self.has_completed and self.has_succeeded:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class StreamTransferContext(TransferContext):

    def __init__(self, ref, urls, save_id, multi):
        TransferContext.__init__(self, multi)
        self.mem_buffer = ""
        self.ref = ref
        self.urls = urls
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False
        self.fifo_dir = tempfile.mkdtemp()
        self.fifo_name = os.path.join(self.fifo_dir, 'fetch_fifo')
        with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
            self.sinkfile_name = sinkfile.name
            
        os.mkfifo(self.fifo_name)
        self.fifo_fd = os.open(self.fifo_name, os.O_RDWR | os.O_NONBLOCK)
        self.sink_fp = open(self.sinkfile_name, "wb")
        self.current_start_byte = 0
        self.chunk_size = 1048576
        self.have_written_to_process = False
        self.response_had_stream = False
        self.dormant_until = None
        self.requests_paused = False
        self.request_length = None

        self.save_id = save_id
        self.multi = multi
        self.description = self.urls[0]
        multi.handles.append(self)
        self.start_next_fetch()

    def close_fifo(self):
        if self.fifo_fd == -1:
            return
        os.close(self.fifo_fd)
        self.fifo_fd = -1
        self.mem_buffer = ""

    def fifo_closed(self):
        self.close_fifo()
        if self.requests_paused and not self.has_completed:
            self.requests_paused = False
            self.consider_restart()

    def write_without_blocking(self, fd, _str):

        if self.fifo_fd == -1:
            # Silently swallow data; the other end is gone.
            return len(_str)
        total_written = 0
        while len(_str) > 0:
            try:
                bytes_written = os.write(self.fifo_fd, _str)
                _str = _str[bytes_written:]
                total_written += bytes_written
            except OSError, e:
                # Note that we can't get EPIPE here, as we ourselves hold a read handle.
                if e.errno == EAGAIN:
                    return total_written
                else:
                    raise
        return total_written

    def write_data(self, _str):
        
        if len(self.mem_buffer) > 0:
            self.mem_buffer += _str
        else:
            written = self.write_without_blocking(self.fifo_fd, _str)
            if written < len(_str):
                self.mem_buffer += _str[written:]
        ret = self.sink_fp.write(_str)
        cherrypy.log.error("Now at position %d (result of write was %s)" % (self.sink_fp.tell(), str(ret)), 'CURL_FETCH', logging.DEBUG)
        self.current_start_byte += len(_str)
        self.have_written_to_process = True

    def start_next_fetch(self):
        cherrypy.log.error("Fetch %s offset %d" % (self.description, self.current_start_byte), 'CURL_FETCH', logging.DEBUG)
        self.start_fetch(self.urls[self.failures], 
                         (self.current_start_byte, 
                          self.current_start_byte + self.chunk_size))

    def consider_restart(self):
        if self.dormant_until is None and not self.requests_paused:
            self.start_next_fetch()

    def wake_up(self):
        self.dormant_until = None
        cherrypy.log.error("Wakeup %s fetch; considering restart" % self.description, 'CURL_FETCH', logging.DEBUG)
        self.consider_restart()

    def try_empty_buffer(self):

        written = self.write_without_blocking(self.fifo_fd, self.mem_buffer)
        self.mem_buffer = self.mem_buffer[written:]
        if len(self.mem_buffer) == 0 and self.requests_paused:
            cherrypy.log.error("Consumer buffer empty for %s, considering restart" % self.description, 'CURL_FETCH', logging.DEBUG)
            self.requests_paused = False
            self.consider_restart()
            return True
        return False

    def write_header_line(self, _str):
        if _str.startswith("Pragma") != -1 and _str.find("streaming") != -1:
            self.response_had_stream = True
        match_obj = length_regex.match(_str)
        if match_obj is not None:
            self.request_length = int(match_obj.group(1))

    def start_fetch(self, url, range):
        self.response_had_stream = False
        TransferContext.start_fetch(self, url, range)

    def pause_for(self, secs):
        self.dormant_until = datetime.now() + timedelta(0, secs)
        cherrypy.log.error("Pausing %s fetch due to producer buffer empty" % self.description, 'CURL_FETCH', logging.DEBUG)

    def success(self):

        cherrypy.log.error("Fetch %s succeeded (length %d)" % (self.description, self.request_length), "CURL_FETCH", logging.DEBUG)
 
        if len(self.mem_buffer) == 0:
            if self.request_length is not None and self.request_length < (self.chunk_size / 2):
                self.pause_for(1)
            else:
                self.start_next_fetch()
            self.request_length = None
        else:
            # We should hold off on ordering another chunk until the process has consumed this one
            # The restart will happen when the buffer drains.
            cherrypy.log.error("Pausing %s fetch consumer buffer full" % self.description, 'CURL_FETCH', logging.DEBUG)
            self.requests_paused = True

    def failure(self, errno, errmsg):

        if errno == 416:
            if not self.response_had_stream:
                self.close_fifo()
                self.has_completed = True
                self.has_succeeded = True
            else:
                # We've got ahead of the producer.
                # Pause for 1 seconds.
                self.pause_for(1)

        else:
            cherrypy.log.error("Fetch %s failed (error %s)" % (self.description, str(errno)), "CURL_FETCH", logging.WARNING)
            if self.have_written_to_process:
                # Can't just fail-over; we've failed after having written bytes to a process.
                self.close_fifo()
                self.has_completed = True
                self.has_succeeded = False
            else:
                self.failures += 1
                try:
                    self.start_fetch(self.urls[self.failures], (0, self.chunk_size))
                except IndexError:
                    # Run out of URLs to try
                    self.close_fifo()
                    self.has_completed = True
                    self.has_succeeded = False

    def cleanup(self):
        TransferContext.cleanup(self)
        cherrypy.log.error('Closing sink file for %s (wrote %d bytes, tell = %d, errors = %s)' % (self.save_id, self.current_start_byte, self.sink_fp.tell(), str(self.sink_fp.errors)), 'CURL_FETCH', logging.DEBUG)
        self.sink_fp.flush()
        self.sink_fp.close()
        cherrypy.log.error('File now closed (errors = %s)' % self.sink_fp.errors, 'CURL_FETCH', logging.DEBUG)
        os.unlink(self.fifo_name)
        os.rmdir(self.fifo_dir)

    def save_result(self, block_store):
        if self.has_completed and self.has_succeeded:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class BlockStore:
    
    def __init__(self, hostname, port, base_dir, ignore_blocks=False):
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
    
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
        self.object_cache[id] = object
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
        elif isinstance(ref, SWNullReference):
            raise RuntimeSkywritingError()
        elif isinstance(ref, SWDataValue):
            id = self.allocate_new_id()
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

        fetch_ctx = TransferSetContext()

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference) or isinstance(ref, SW2_FetchReference):
                save_id = ref.id
            else:
                save_id = self.allocate_new_id()
            return FileTransferContext(urls, save_id, fetch_ctx)

        request_list = []
        for (ref, resolution) in zip(refs, resolved_refs):
            if resolution is None:
                request_list.append(create_transfer_context(ref))

        fetch_ctx.transfer_all()

        for req in request_list:
            req.save_result(self)

        result_list = []
 
        for resolution in resolved_refs:
            if resolution is None:
                next_req = request_list.pop(0)
                next_req.cleanup()
                result_list.append(self.filename(next_req.save_id))
            else:
                result_list.append(resolution)

        fetch_ctx.cleanup()

        failure_bindings = fetch_ctx.get_failed_refs()
        if failure_bindings is not None:
            raise MissingInputException(failure_bindings)

        return result_list

    def retrieve_filenames_for_refs(self, refs):

        fetch_ctx = StreamTransferSetContext()

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamReference):
                save_id = ref.id
            else:
                save_id = self.allocate_new_id()
            return StreamTransferContext(ref, urls, save_id, fetch_ctx)

        result_list = []
 
        for (ref, resolution) in zip(refs, resolved_refs):
            if resolution is None:
                ctx = create_transfer_context(ref)
                result_list.append(ctx.fifo_name)
            else:
                result_list.append(resolution)

        return (result_list, fetch_ctx)

    def retrieve_objects_for_refs(self, refs, decoder):
        
        easy_solutions = [self.try_retrieve_object_for_ref_without_transfer(ref, decoder) for ref in refs]
        fetch_urls = [self.get_fetch_urls_for_ref(ref) for ref in refs]

        result_list = []
        request_list = []
        
        transfer_ctx = TransferSetContext()

        for (solution, this_fetch_urls) in zip(easy_solutions, fetch_urls):
            if solution is not None:
                result_list.append(solution)
            else:
                result_list.append(None)
                request_list.append((ref, BufferTransferContext(this_fetch_urls, transfer_ctx)))

        transfer_ctx.transfer_all()

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
        transfer_ctx.cleanup()
        
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
            ref = SW2_ConcreteReference(id, SWTaskOutputProvenance(task_id, -1), None)
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
            
            ref = SW2_ConcreteReference(id, SWTaskOutputProvenance(task_id, -1), size)
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
            ret.append(SW2_ConcreteReference(id, SWNoProvenance(), os.path.getsize(self.filename(id)), [self.netloc]))
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
