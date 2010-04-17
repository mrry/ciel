'''
Created on 14 Apr 2010

@author: dgm36
'''
from threading import Lock
from cherrypy.lib.static import serve_file
import urllib
import urllib2
import shutil
import pickle
import os
import cherrypy

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from mrry.mercator.runtime.task_executor import build_reference_from_tuple,\
    SWRealReference
urlparse.uses_netloc.append("swbs")

class SWReferenceJSONEncoder(simplejson.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, SWRealReference):
            return {'__ref__': obj.as_tuple()}
        else:
            return simplejson.JSONEncoder.default(self, obj)

class BlockStore:
    
    def __init__(self, hostname, port, base_dir, master_proxy):
        self._lock = Lock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.current_id = 0
        self.object_cache = {}
        
        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle}
        
    def json_decode_object_hook(self, dict_):
        if '__ref__' in dict_:
            return build_reference_from_tuple(dict_['__ref__'])
        else:
            return dict_
    
    def encode_noop(self, obj, file):
        return file.write(obj)
    def decode_noop(self, file):
        return file.read()    
    def encode_json(self, obj, file):
        return simplejson.dump(obj, file, cls=SWReferenceJSONEncoder)
    def decode_json(self, file):
        return simplejson.load(file, object_hook=self.json_decode_object_hook)
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
    
    def publish_global_object(self, global_id, url):
        self.master_proxy.publish_global_object(global_id, [url])
    
    def store_object(self, object, encoder):
        """Stores the given object as a block, and returns a swbs URL to it."""
        with self._lock:
            id = self.allocate_new_id()
            self.object_cache[id] = object
        with open(self.filename(id), "wb") as object_file:
            self.encoders[encoder](object, object_file)
        return 'swbs://%s/%d' % (self.netloc, id)
    
    def store_file(self, filename):
        """Stores the file with the given local filename as a block, and returns a swbs URL to it."""
        with self._lock:
            id = self.allocate_new_id()
        shutil.copyfile(filename, self.filename(id))
        return 'swbs://%s/%d' % (self.netloc, id)
    
    def retrieve_object_by_url(self, url, decoder):
        """Returns the object referred to by the given URL."""
        parsed_url = urlparse.urlparse(url)
        print url, '<--->', parsed_url
        
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
        print ret
        object_file.close()
        return ret
                
    def retrieve_filename_by_url(self, url):
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
        
        with self._lock:
            id = self.allocate_new_id()
            
        filename = self.filename(id)
        urllib.urlretrieve(fetch_url, filename)
        
        return self.filename
    
