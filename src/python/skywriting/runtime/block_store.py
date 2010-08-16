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

'''
Created on 14 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from threading import Lock
from urllib2 import URLError, HTTPError
from skywriting.runtime.exceptions import ExecutionInterruption
import random
import urllib2
import shutil
import pickle
import os
import uuid
import tempfile

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from skywriting.runtime.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SWNullReference, SWURLReference, ACCESS_SWBS
urlparse.uses_netloc.append("swbs")

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
        id = uuid.UUID(hex=parsed_url.path[1:])
        return 'http://%s/data/%s' % (parsed_url.netloc, str(id))
    else:
        return url

class BlockStore:
    
    def __init__(self, hostname, port, base_dir, master_proxy):
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
    
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
    
    def allocate_new_id(self):
        return uuid.uuid1()
    
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
    
    CACHE_SIZE_LIMIT=1024
    def store_url_in_cache(self, url, filename):
        with self._lock:
            if len(self.url_cache_filenames) == BlockStore.CACHE_SIZE_LIMIT:
                self.evict_lru_from_url_cache()
            self.url_cache_filenames[url] = filename
            self.mark_url_as_accessed(url)
    
    def filename(self, id):
        return os.path.join(self.base_dir, str(id))
    
    def publish_global_refs(self, global_id, refs, size_hint=None):
        self.master_proxy.publish_global_refs(global_id, refs)
    
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

    def retrieve_object_by_url(self, url, decoder):
        """Returns the object referred to by the given URL."""
        filename = self.find_url_in_cache(url)
        if filename is None:
            parsed_url = urlparse.urlparse(url)
            if parsed_url.scheme == 'swbs':
                id = uuid.UUID(hex=parsed_url.path[1:])
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
                    fetch_url = 'http://%s/data/%s' % (parsed_url.netloc, str(id))
    
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
                
    def retrieve_filename_by_url(self, url, size_limit=None):
        """Returns the filename of a file containing the data at the given URL."""
        filename = self.find_url_in_cache(url)
        id = None
        if filename is None:
            parsed_url = urlparse.urlparse(url)
            
            if parsed_url.scheme == 'swbs':
                id = uuid.UUID(hex=parsed_url.path[1:])
                if parsed_url.netloc == self.netloc:
                    # Retrieve local object.
                    return self.filename(id)
                else:
                    # Retrieve remote in-system object.
                    # XXX: should extract this magic string constant.
                    fetch_url = 'http://%s/data/%s' % (parsed_url.netloc, str(id))
            else:
                # Retrieve remote ex-system object.
                fetch_url = url
            
            headers = {}
            if size_limit is not None:
                headers['If-Match'] = str(size_limit)
            
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
 
            self.store_url_in_cache(url, filename)
        
        return filename
    
    def retrieve_filename_for_concrete_ref(self, ref):
        netloc = self.choose_best_netloc(ref.location_hints.keys())
        access_method = self.choose_best_access_method(ref.location_hints[netloc])
        assert access_method == ACCESS_SWBS
        return self.retrieve_filename_by_url('swbs://%s/%s' % (netloc, str(ref.id)))
        
    def retrieve_filename_for_ref(self, ref):
        assert isinstance(ref, SWRealReference)
        if isinstance(ref, SW2_ConcreteReference):
            return self.retrieve_filename_for_concrete_ref(ref)
        elif isinstance(ref, SWURLReference):
            for url in ref.urls:
                filename = self.find_url_in_cache(url)
                if filename is not None:
                    return filename
            chosen_url = self.choose_best_url(ref.urls)
            return self.retrieve_filename_by_url(chosen_url)
        elif isinstance(ref, SWDataValue):
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
        netloc = self.choose_best_netloc(ref.location_hints.keys())
        access_method = self.choose_best_access_method(ref.location_hints[netloc])
        assert access_method == ACCESS_SWBS
        return self.retrieve_object_by_url('swbs://%s/%s' % (netloc, str(ref.id)), decoder)
        
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
            raise
        elif isinstance(ref, SWNullReference):
            raise
        else:
            print ref
            raise        
        
    def choose_best_netloc(self, netlocs):
        if len(netlocs) == 1:
            return netlocs[0]
        else:
            for netloc in netlocs:
                if netloc == self.netloc:
                    return netloc
            return random.choice(netlocs)
        
    def choose_best_access_method(self, methods):
        assert ACCESS_SWBS in methods
        return ACCESS_SWBS
        
    def choose_best_url(self, urls):
        if len(urls) == 1:
            return urls[0]
        else:
            for url in enumerate(urls):
                parsed_url = urlparse.urlparse(url)
                if parsed_url.netloc == self.netloc:
                    return url
            return random.choice(urls)
        
    def generate_block_list_file(self):
        with tempfile.NamedTemporaryFile('w', delete=False) as block_list_file:
            filename = block_list_file.name
            for block_name in os.listdir(self.base_dir):
                try:
                    block_id = uuid.UUID(hex=block_name)
                    block_list_file.write(block_id.bytes)
                except:
                    pass
                
        return filename
