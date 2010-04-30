'''
Created on 14 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from threading import Lock
from urllib2 import URLError, HTTPError
from mrry.mercator.runtime.exceptions import ExecutionInterruption,\
    ReferenceUnavailableException
import random
import urllib2
import shutil
import pickle
import os

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from mrry.mercator.runtime.references import SWRealReference,\
    build_reference_from_tuple
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

class BlockStore:
    
    def __init__(self, hostname, port, base_dir, master_proxy):
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.current_id = 0
        self.object_cache = {}
        
        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle}
    
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
        ret = self.current_id
        self.current_id += 1
        return ret
    
    def filename(self, id):
        return os.path.join(self.base_dir, str(id))
    
    def publish_global_refs(self, global_id, refs, size_hint=None):
        self.master_proxy.publish_global_refs(global_id, refs)
    
    def store_raw_file(self, incoming_fobj):
        with self._lock:
            id = self.allocate_new_id()
        with open(self.filename(id), "wb") as data_file:
            shutil.copyfileobj(incoming_fobj, data_file)
            file_size = data_file.tell()
        return 'swbs://%s/%d' % (self.netloc, id), file_size            
    
    def store_object(self, object, encoder):
        """Stores the given object as a block, and returns a swbs URL to it."""
        with self._lock:
            id = self.allocate_new_id()
            self.object_cache[id] = object
        with open(self.filename(id), "wb") as object_file:
            self.encoders[encoder](object, object_file)
            file_size = object_file.tell()
        return 'swbs://%s/%d' % (self.netloc, id), file_size
    
    def store_file(self, filename, can_move=False):
        """Stores the file with the given local filename as a block, and returns a swbs URL to it."""
        with self._lock:
            id = self.allocate_new_id()
        if can_move:
            shutil.move(filename, self.filename(id))
        else:
            shutil.copyfile(filename, self.filename(id))
        file_size = os.path.getsize(self.filename(id))
        return 'swbs://%s/%d' % (self.netloc, id), file_size
    
    def retrieve_object_by_url(self, url, decoder):
        """Returns the object referred to by the given URL."""
        parsed_url = urlparse.urlparse(url)
        if parsed_url.scheme == 'swbs':
            id = int(parsed_url.path[1:])
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
                fetch_url = 'http://%s/data/%d' % (parsed_url.netloc, id)

        else:
            # Retrieve remote ex-system object.
            fetch_url = url

        object_file = urllib2.urlopen(fetch_url)
        ret = self.decoders[decoder](object_file)
        object_file.close()
        return ret
                
    def retrieve_filename_by_url(self, url, size_limit=None):
        """Returns the filename of a file containing the data at the given URL."""
        parsed_url = urlparse.urlparse(url)
        
        if parsed_url.scheme == 'swbs':
            id = int(parsed_url.path[1:])
            if parsed_url.netloc == self.netloc:
                # Retrieve local object.
                return self.filename(id)
            else:
                # Retrieve remote in-system object.
                # XXX: should extract this magic string constant.
                fetch_url = 'http://%s/data/%d' % (parsed_url.netloc, id)
        else:
            # Retrieve remote ex-system object.
            fetch_url = url
        
        headers = {}
        if size_limit is not None:
            headers['If-Match'] = str(size_limit)
        
        request = urllib2.Request(fetch_url, headers=headers)
        
        try:
            response = urllib2.urlopen(request)
        except HTTPError as ue:
            if ue.code == 412:
                raise ExecutionInterruption()
            else:
                raise
        except URLError:
            raise
        
        with self._lock:
            id = self.allocate_new_id()
   
        filename = self.filename(id)

        with open(filename, 'wb') as data_file:
            shutil.copyfileobj(response, data_file)
        
        return filename
    
    def choose_best_url(self, urls):
        if len(urls) == 1:
            return urls[0]
        else:
            for url in enumerate(urls):
                parsed_url = urlparse.urlparse(url)
                if parsed_url.netloc == self.netloc:
                    return url
            return random.choice(urls)
