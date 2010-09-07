# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from __future__ import with_statement
from threading import Lock
from urllib2 import URLError, HTTPError
from skywriting.runtime.exceptions import ExecutionInterruption,\
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
from datetime import datetime
from cStringIO import StringIO
from errno import EAGAIN

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from skywriting.runtime.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SWNullReference, SWURLReference, \
    SWNoProvenance, SWTaskOutputProvenance, SW2_StreamReference,\
    SW2_TombstoneReference
import hashlib
import contextlib
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

class StreamRetry:
    pass
STREAM_RETRY = StreamRetry()

def get_netloc_for_sw_url(url):
    return urlparse.urlparse(url).netloc

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
        self.curl_ctx.setopt(pycurl.M_PIPELINING, 1)
        self.curl_ctx.setopt(pycurl.M_MAXCONNECTS, 20)

    def register_handle(self, ctx):
        self.handles.append(ctx)

    def unregister_handle(self, ctx):
        self.handles.remove(ctx)

    def start_fetch(ctx):
        self.curl_ctx.add_handle(ctx.curl_ctx)

    def process(self):
        while 1:
            # Will generate write_data callbacks as appropriate
            ret, num_handles = self.curl_ctx.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break        
        while 1:
            num_q, ok_list, err_list = self.curl_ctx.info_read()
            for c in ok_list:
                self.curl_ctx.remove_handle(c)
                c.ctx._success()
            for c, errno, errmsg in err_list:
                self.curl_ctx.remove_handle(c)
                c.ctx._failure(errno, errmsg)
            if num_q == 0:
                break

    def transfer_all(self):
        while 1:
            self.process()
            self.select(5.0)

    def cleanup(self):
        self.curl_ctx.close()

class StreamTransferSetContext(TransferSetContext):
    def __init__(self):
        TransferSetContext.__init__(self)
        self.handles = []

    def writable_handles(self):
        return filter(self.handles, lambda x: (not x.has_completed) and len(x.mem_buffer) > 0)

    def dormant_handles(self):
        return filter(self.handles, lambda x: x.dormant_until is not None)

    def process(self):
        TransferSetContext.process(self)
        for handle in self.writable_handles():
            handle.try_empty_buffer()
        for handle in self.dormant_handles():
            if datetime.now() > handle.dormant_until:
                handle.dormant_until = None
                handle.start_next_fetch()

    def select(self):
        (read, write, exn) = self.curl_ctx.fdset()
        for handle in self.writable_handles():
            write.append(handle.fifo_fd)
        select.select(read, write, exn)

    def transfer_all(self):
        while 1:
            self.process()
            self.select()
            remaining_transfers = filter(self.handles, lambda x: not x.has_completed)
            if len(remaining_transfers == 0):
                return

    def cleanup(self, block_store):
        for handle in self.handles:
            handle.save_result(block_store)
            handle.cleanup()
        TransferSetContext.cleanup()

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
        self.curl_ctx.setopt(pycurl.URL, url)
        if range is not None:
            self.curl_ctx.setopt(pycurl.HEADER, "Range: bytes=%d-%d" % range)
        self.multi.start_fetch(self)
            
    def _success(self):
        self.active = False
        self.success()

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
        self.start_fetch(self, self.urls[0])

    def write_data(self, str):
        self.buffer.write(str)

    def write_header_line(self, str):
        pass

    def success(self):
        self.has_completed = True
        self.has_succeeded = True

    def failure(self):
        self.failures += 1
        try:
            self.start_fetch(self.urls[self.failures])
            self.buffer.close()
            self.buffer = StringIO()
        except IndexError:
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
        self.start_fetch(self, self.urls[0])

    def write_data(self, str):
        self.sink_fp.write(str)

    def write_header_line(self, str):
        pass

    def success(self):
        self.has_completed = True
        self.has_succeeded = True

    def failure(self):
        self.failures += 1
        try:
            self.sink_fp.seek(0)
            self.sink_fp.truncate(0)
            self.start_fetch(self.urls[self.failures])
            
        except IndexError:
            self.has_completed = True
            self.has_succeeded = False

    def cleanup(self):
        self.sink_fp.close()
        TransferContext.cleanup(self)

    def save_result(self, block_store):
        if self.has_completed:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class StreamTransferContext(TransferContext):

    def __init__(self, urls, save_id, multi):
        TransferContext.__init__(self)
        self.mem_buffer = ""
        self.urls = urls
        self.failures = 0
        self.has_completed = False
        self.has_succeeded = False
        self.fifo_dir = tempfile.mkdtemp()
        self.fifo_name = os.path.join(self.fifo_dir, 'fetch_fifo')
        with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
            self.sinkfile_name = sinkfile.name
            
        os.mkfifo(self.fifo_name)
        self.fifo_fd = os.open(self.fifo_name, os.O_WRONLY | os.O_NONBLOCK)
        self.sink_fp = open(self.sinkfile_name, "wb")
        self.current_start_byte = 0
        self.chunk_size = 1048576
        self.have_written_to_process = False
        self.response_had_stream = False
        self.dormant_until = None

        self.ref = ref
        self.multi = multi
        multi.handles.append(self)
        self.start_next_fetch()

    def write_without_blocking(self, fd, str):

        total_written = 0
        while len(str) > 0:
            try:
                bytes_written = os.write(self.fifo_fd, str)
                str = str[bytes_written:]
                total_written += bytes_written
            except OSError, errno:
                if errno == EAGAIN:
                    return total_written
                else:
                    raise

    def write_data(self, str):
        
        if len(self.mem_buffer) > 0:
            self.mem_buffer.append(str)
        else:
            written = write_without_blocking(self, self.fifo_fd, str)
            if written < len(str):
                self.mem_buffer.append(str[written:])
        write(self.sink_fp, str)
        self.current_start_byte += len(str)
        self.have_written_to_process = True

    def start_next_fetch(self):
        self.start_fetch(self.urls[self.failures], 
                         (self.current_start_byte, 
                          self.current_start_byte + self.chunk_size))

    def try_empty_buffer(self):

        written = write_without_blocking(self, self.fifo_fd, self.mem_buffer)
        self.mem_buffer = self.mem_buffer[written:]
        if len(self.mem_buffer) == 0:
            self.start_next_fetch()

    def write_header_line(self, str):
        if str.find("Pragma") != -1 and str.find("streaming") != -1:
            self.response_had_stream = True

    def start_fetch(self, url, range):
        self.response_had_stream = False
        TransferContext.start_fetch(self, url, range)

    def success(self):
 
        if len(self.mem_buffer) == 0:
            self.start_next_fetch()
        else:
            # We should hold off on ordering another chunk until the process has consumed this one
            # The restart will happen when the buffer drains.
            pass

    def failure(self, errno, errmsg):

        if errno == 416:
            if not self.response_had_stream:
                self.has_completed = True
                self.has_succeeded = True
            else:
                # Pause for 5 seconds
                self.dormant_until = datetime.now() + timedelta(0, 5)
        else:
            if self.have_written_to_process:
                # Can't just fail-over; we've failed after having written bytes to a process.
                self.has_completed = True
                self.has_succeeded = False
            else:
                self.failures += 1
                try:
                    self.start_fetch(self.urls[self.failures], (0, self.chunk_size))
                except IndexError:
                    # Run out of URLs to try
                    self.has_completed = True
                    self.has_succeeded = False

    def cleanup(self):
        TransferContext.cleanup(self)
        os.close(self.fifo_fd)
        self.sink_fp.close()
        os.unlink(self.fifo_name)
        os.rmdir(self.fifo_dir)

    def save_result(self, block_store):
        if self.has_completed:
            block_store.store_file(self.sinkfile_name, self.save_id, True)

class BlockStore:
    
    def __init__(self, hostname, port, base_dir):
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
    
        # Maintains a set of block IDs that are currently being written.
        # (i.e. They are in the pre-publish/streamable state, and have not been
        #       committed to the block store.)
        self.streaming_id_set = set()
    
        self.current_cache_access_id = 0
        self.url_cache_filenames = {}
        self.url_cache_access_times = {}
        
        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle, 'handle': self.decode_handle}
    
    def decode_handle(self, file):
        return file
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
    
    def streaming_filename(self, id):
        os.path.join(self.base_dir, '.%s' % id)
    
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
        with self._lock:
            self.streaming_id_set.add(id)
            os.symlink(filename, self.streaming_filename(id))
                   
    def commit_file(self, filename, id, can_move=False):
        if can_move:
            # Moving the file under the lock should be cheap.
            # N.B. We need to protect all operations because the streaming
            #      filename will be unlinked.
            with self._lock:
                self.store_file(filename, id, True)
                self.streaming_id_set.remove(id)
                os.symlink(self.filename(id), self.streaming_filename(id))
        else:
            # A copy will be necessary, so do this outside the lock.
            self.store_file(filename, id, False)
            with self._lock:
                self.streaming_id_set.remove(id)
                os.symlink(self.filename(id), self.streaming_filename(id))
            
        url, file_size = self.store_file(filename, id, False)
        
        
        return url, file_size

    def rollback_file(self, id):
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
            raise
        elif isinstance(ref, SWNullReference):
            raise
        elif isinstance(ref, SWDataValue):
            id = self.allocate_new_id()
            with open(self.filename(id), 'w') as obj_file:
                self.encode_json(ref.value, obj_file)
            return self.filename(id)
        elif isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamingReference):
            for loc in ref.location_hints:
                if loc == self.netloc:
                    return self.filename(ref.id)
            check_urls = ["swbs://%s/%s" % (loc_hint, str(ref.id)) for loc_hint in ref.location_hints]
            return find_first_cached(check_urls)
        elif isinstance(ref, SWURLReference):
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

        if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamingReference)
            return ["http://%s/data/%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
        elif isinstance(ref, SWURLReference):
            return map(sw_to_external_url, ref.urls)
                
    def retrieve_filenames_for_refs_eager(self, refs):

        fetch_ctx = TransferSetContext()

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamingReference):
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
 
        for resolution in resovled_refs:
            if resolution is None:
                next_req = request_list.pop(0)
                next_req.cleanup()
                result_list.append(self.filename(next_req.save_id))
            else:
                result_list.append(resolution)

        fetch_ctx.cleanup()

        return result_list

    def retrieve_filenames_for_refs(self, refs):

        fetch_ctx = StreamTransferSetContext()
      
        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        def create_transfer_context(ref):
            urls = self.get_fetch_urls_for_ref(ref)
            if isinstance(ref, SW2_ConcreteReference) or isinstance(ref, SW2_StreamingReference):
                save_id = ref.id
            else:
                save_id = self.allocate_new_id()
            return StreamTransferContext(urls, save_id, fetch_ctx)

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

        for (solution, fetch_urls) in zip(easy_solutions, fetch_urls):
            if solution is not None:
                result_list.append(solution)
            else:
                result_list.append(None)
                request_list.append(BufferTransferContext(fetch_urls, transfer_ctx))

        transfer_ctx.transfer_all()

        j = 0
        for i, res in enumerate(result_list):
            if res is None:
                next_object = request_list[j]
                j += 1
                if next_object.has_succeeded:
                    next_object.buffer.seek(0)
                    result_list[i] = self.decoders[decoder](next_object.buffer)

        for req in request_list:
            req.cleanup()
        transfer_ctx.cleanup()

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
    
    def generate_block_list_file(self):
        cherrypy.log.error('Generating block list file', 'BLOCKSTORE', logging.INFO)
        with tempfile.NamedTemporaryFile('w', delete=False) as block_list_file:
            filename = block_list_file.name
            for block_name, block_size in self.block_list_generator():
                block_list_file.write(BLOCK_LIST_RECORD_STRUCT.pack(block_name, block_size))
        return filename

    def is_empty(self):
        return len(os.listdir(self.base_dir)) == 0
