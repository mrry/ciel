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
    
    def retrieve_object_by_url(self, url, decoder):
        """Returns the object referred to by the given URL."""
        filename = self.find_url_in_cache(url)
        if filename is None:
            parsed_url = urlparse.urlparse(url)
            if parsed_url.scheme == 'swbs':
                id = parsed_url.path[1:]
                if parsed_url.netloc == self.netloc:
                    # Retrieve local object.
                    try:
                        return self.object_cache[id]
                    except KeyError:
                        with open(self.filename(id)) as object_file:
                            return self.decoders[decoder](object_file)
                else:
                    # Retrieve remote in-system object.
                    # XXX: should extract this magic string constant.
                    fetch_url = 'http://%s/data/%s' % (parsed_url.netloc, id)
    
            else:
                # Retrieve remote ex-system object.
                fetch_url = url

            object_file = urllib2.urlopen(fetch_url)
        else:
            object_file = open(filename, "r")

        ret = self.decoders[decoder](object_file)
        if(decoder != "handle"):
            object_file.close()
        return ret
                
    def is_available_locally(self, ref):
        return self.netloc in ref.location_hints or (isinstance(ref, SW2_ConcreteReference) and os.path.exists(self.filename(ref.id)))

    def retrieve_filename_by_url(self, url, size_limit=None):
        """Returns the filename of a file containing the data at the given URL."""
        filename = self.find_url_in_cache(url)
        id = None
        if filename is None:
            parsed_url = urlparse.urlparse(url)
            
            if parsed_url.scheme == 'swbs':
                id = parsed_url.path[1:]
                if parsed_url.netloc == self.netloc:
                    # Retrieve local object.
                    cherrypy.engine.publish("worker_event", "Block store: SWBS target was local already")
                    return self.filename(id)
                else:
                    # Retrieve remote in-system object.
                    # XXX: should extract this magic string constant.
                    fetch_url = 'http://%s/data/%s' % (parsed_url.netloc, id)
            else:
                # Retrieve remote ex-system object.
                fetch_url = url
            
            headers = {}
            if size_limit is not None:
                headers['If-Match'] = str(size_limit)
            
            cherrypy.engine.publish("worker_event", "Block store: fetching " + fetch_url)
            request = urllib2.Request(fetch_url, headers=headers)
            
            try:
                response = urllib2.urlopen(request)
            except HTTPError, ue:
                if ue.code == 412:
                    raise ExecutionInterruption()
                else:
                    raise
            except URLError:
                raise
            
            if id is None:
                id = self.allocate_new_id()
       
            filename = self.filename(id)
    
            with open(filename, 'wb') as data_file:
                shutil.copyfileobj(response, data_file)
 
            response.close()
 
            self.store_url_in_cache(url, filename)
        else:
            cherrypy.engine.publish("worker_event", "Block store: URL hit in cache")

        return filename
    
    def retrieve_filename_for_concrete_ref(self, ref):
        netloc = self.choose_best_netloc(ref.location_hints)
        try:
            result = self.retrieve_filename_by_url('swbs://%s/%s' % (netloc, str(ref.id)))
        except:
            alternative_netlocs = ref.location_hints.copy()
            alternative_netlocs.remove(netloc)
            while len(alternative_netlocs) > 0:
                netloc = self.choose_best_netloc(alternative_netlocs)
                try:
                    result = self.retrieve_filename_by_url('swbs://%s/%s' % (netloc, str(ref.id)))
                    break
                except:
                    alternative_netlocs.remove(netloc)
                
            if len(alternative_netlocs) == 0:
                raise MissingInputException({ ref.id : [SW2_TombstoneReference(ref.id, ref.location_hints)] })
        
        return result
        
    def retrieve_filename_for_ref(self, ref):
        assert isinstance(ref, SWRealReference)
        if isinstance(ref, SW2_ConcreteReference):
            cherrypy.engine.publish("worker_event", "Block store: fetch SWBS")
            return self.retrieve_filename_for_concrete_ref(ref)
        elif isinstance(ref, SWURLReference):
            for url in ref.urls:
                filename = self.find_url_in_cache(url)
                if filename is not None:
                    cherrypy.engine.publish("worker_event", "Block store: URL hit in cache")
                    return filename
            chosen_url = self.choose_best_url(ref.urls)
            cherrypy.engine.publish("worker_event", "Block store: fetch URL " + chosen_url)
            return self.retrieve_filename_by_url(chosen_url)
        elif isinstance(ref, SWDataValue):
            cherrypy.engine.publish("worker_event", "Block store: writing datavalue to file")
            id = self.allocate_new_id()
            with open(self.filename(id), 'w') as obj_file:
                self.encode_json(ref.value, obj_file)
            return self.filename(id)
        elif isinstance(ref, SWErrorReference):
            raise
        elif isinstance(ref, SWNullReference):
            raise
        else:
            raise
        
    def retrieve_object_for_concrete_ref(self, ref, decoder):
        netloc = self.choose_best_netloc(ref.location_hints)
        try:
            result = self.retrieve_object_by_url('swbs://%s/%s' % (netloc, str(ref.id)), decoder)
        except:
            
            alternative_netlocs = ref.location_hints.copy()
            alternative_netlocs.remove(netloc)
            while len(alternative_netlocs) > 0:
                netloc = self.choose_best_netloc(alternative_netlocs)
                try:
                    result = self.retrieve_object_by_url('swbs://%s/%s' % (netloc, str(ref.id)), decoder)
                    break
                except:
                    alternative_netlocs.remove(netloc)
                    
            if len(alternative_netlocs) == 0:
                raise MissingInputException({ ref.id : [SW2_TombstoneReference(ref.id, ref.location_hints)] })
        
        return result
        
    def retrieve_object_for_ref(self, ref, decoder):
        assert isinstance(ref, SWRealReference)
        if isinstance(ref, SW2_ConcreteReference):
            return self.retrieve_object_for_concrete_ref(ref, decoder)
        elif isinstance(ref, SWURLReference):
            for url in ref.urls:
                filename = self.find_url_in_cache(url)
                if filename is not None:
                    with open(filename) as f:
                        ret = self.decoders[decoder](f)
                    return ret
            chosen_url = self.choose_best_url(ref.urls)
            return self.retrieve_object_by_url(chosen_url, decoder)
        elif isinstance(ref, SWDataValue):
            assert decoder == 'json'
            return ref.value
        elif isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()
        elif isinstance(ref, SWNullReference):
            raise
        else:
            print ref
            raise        
        
    def retrieve_range_for_ref(self, ref, start, chunk_size):
        assert isinstance(ref, SW2_StreamReference) or isinstance(ref, SW2_ConcreteReference)
        netloc = self.choose_best_netloc(ref.location_hints)
    
        fetch_url = 'http://%s/data/%s' % (netloc, ref.id)
        range_header = 'bytes=%d-%d' % (start, start + chunk_size - 1)
        headers = {'Range': range_header}
        # TODO: Some event publishing.
        request = urllib2.Request(fetch_url, headers=headers)
            
        try:
            response = urllib2.urlopen(request)
        except HTTPError, he:
            if he.code == 416:
                try:
                    streaming = he.info()['Pragma']
                    if streaming == 'streaming':
                        return STREAM_RETRY
                    else:
                        return False
                except KeyError:
                    return False
            else:
                raise he
        
        return response.read()
               
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