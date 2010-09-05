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

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from skywriting.runtime.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue,\
    SWErrorReference, SWNullReference, SWURLReference, ACCESS_SWBS,\
    SWNoProvenance, SWTaskOutputProvenance
import hashlib
import contextlib
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

# This code adapted from pycurl example "retriever-multi.py"

def grab_urls(reqs):

    queue = []

    m = pycurl.CurlMulti()
    m.handles = []
    m.setopt(pycurl.M_PIPELINING, 1)
    m.setopt(pycurl.M_MAXCONNECTS, 20)
    for req in reqs:
        c = pycurl.Curl()
        c.req = req
        c.req["succeeded"] = False
        c.req["failed"] = False
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 5)
        c.setopt(pycurl.CONNECTTIMEOUT, 30)
        c.setopt(pycurl.TIMEOUT, 300)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.URL, str(req["url"]))
        if "save_to" in req:
            c.fp = open(req["save_to"], "wb")
            c.setopt(pycurl.WRITEDATA, c.fp)
        elif "write_callback" in req:
            c.fp = None
            c.setopt(pycurl.WRITEFUNCTION, req["write_callback"])
            c.setopt(pycurl.WRITEDATA, req["write_callback_data"])
        else:
            raise Exception("grab_urls: requests must include either 'save_to' or 'write_callback'")
        m.handles.append(c)
        m.add_handle(c)

    num_processed = 0
    num_urls = len(reqs)
    while num_processed < num_urls:
        # Run the internal curl state machine for the multi stack
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated
        while 1:
            num_q, ok_list, err_list = m.info_read()
            for c in ok_list:
                if c.fp is not None:
                    c.fp.close()
                    c.fp = None
                m.remove_handle(c)
                c.req["succeeded"] = True
            for c, errno, errmsg in err_list:
                if c.fp is not None:
                    c.fp.close()
                    c.fp = None
                m.remove_handle(c)
                c.req["failed"] = True

            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break

        m.select(1.0)

    # Cleanup
    for c in m.handles:
        if c.fp is not None:
            c.fp.close()
            c.fp = None
        c.close()
    m.close()

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
                
    def retrieve_filenames_for_refs(self, refs):

        # Step 1: Resolve simple refs and check for exceptions

        resolved_after_data = []
        for ref in refs:
            assert isinstance(ref, SWRealReference)
            if isinstance(ref, SWDataValue):
                cherrypy.engine.publish("worker_event", "Block store: writing datavalue to file")
                id = self.allocate_new_id()
                with open(self.filename(id), 'w') as obj_file:
                    self.encode_json(ref.value, obj_file)
                resolved_after_data.append(("resolved", self.filename(id)))
            elif isinstance(ref, SWErrorReference):
                raise
            elif isinstance(ref, SWNullReference):
                raise
            elif isinstance(ref, SW2_ConcreteReference):
                resolved_after_data.append(("int-fetch", (ref.location_hints, str(ref.id))))
            elif isinstance(ref, SWURLReference):
                resolved_after_data.append(("ext-fetch", ref.urls))
            else:
                raise

        # Step 2: Check for hits in the URL cache, or due to already-local in-system locations

        resolved_after_cache = []

        def find_first_cached(urls):
            for url in urls:
                filename = self.find_url_in_cache(url)
                if filename is not None:
                    return filename
            return None

        def add_fetch(check_urls, fetch_urls, id=None):
            filename = find_first_cached(check_urls)
            if filename is not None:
                resolved_after_cache.append(("resolved", filename))
            else:
                if id is None:
                    id = self.allocate_new_id()
                resolved_after_cache.append(("fetch", (check_urls, fetch_urls, self.filename(id))))

        for (fetch_type, info) in resolved_after_data:
            if fetch_type == "int-fetch":
                loc_hints, id = info
                fetch_urls = ["http://%s/data/%s" % (loc_hint, id) for loc_hint in loc_hints]
                check_urls = ["swbs://%s/%s" % (loc_hint, id) for loc_hint in loc_hints]
                add_fetch(check_urls, fetch_urls, id)
            elif fetch_type == "ext-fetch":
                add_fetch(info, info)
            else:
                resolved_after_cache.append((fetch_type, info))

        # Step 3: Build a request descriptor for each request needing a real fetch

        fetch_requests = []
        i = 0
        for (action, info) in resolved_after_cache:
            if action == "fetch":
                check_urls, fetch_urls, filename = info
                fetch_requests.append({"check_urls": check_urls, 
                                       "failures": 0, 
                                       "done": False,
                                       "fetch_urls": fetch_urls,
                                       "save_to": filename,
                                       "url": fetch_urls[0]})
            else:
                fetch_requests.append({"done": True,
                                       "save_to": info})
            i += 1

        # Step 4: Try to fetch what we need, failing over to backup URLs as necessary.
        # This isn't the ideal algorithm, as it won't submit retry requests until all the current wave
        # has either succeeded or failed. That would require more pycURL understanding,
        # specifically how to add requests to an ongoing multi-request.

        live_requests = fetch_requests
        while len(live_requests) > 0:
            live_requests = filter(lambda req : not req["done"], live_requests)
            grab_urls(live_requests)
            for req_result in live_requests:
                
                if req_result["succeeded"]:
                    req_result["done"] = True
                    self.store_url_in_cache(req_result["check_urls"][req_result["failures"]], req_result["save_to"])
                else:
                    try:
                        req_result["failures"] += 1
                        req_result["url"] = req_result["fetch_urls"][req_result["failures"]]
                    except IndexError:
                        # Damn
                        req_result["save_to"] = None
                        req_result["done"] = True

        filenames_to_return = [req["save_to"] for req in fetch_requests]
        return filenames_to_return
            
    def retrieve_object_for_concrete_ref(self, ref, decoder):
        netloc = self.choose_best_netloc(ref.location_hints.keys())
        access_method = self.choose_best_access_method(ref.location_hints[netloc])
        assert access_method == ACCESS_SWBS
        try:
            result = self.retrieve_object_by_url('swbs://%s/%s' % (netloc, str(ref.id)), decoder)
        except:
            
            alternative_netlocs = ref.location_hints.copy()
            del alternative_netlocs[netloc]
            while len(alternative_netlocs) > 0:
                netloc = self.choose_best_netloc(alternative_netlocs)
                access_method = self.choose_best_access_method(alternative_netlocs[netloc])
                try:
                    result = self.retrieve_object_by_url('swbs://%s/%s' % (netloc, str(ref.id)), decoder)
                    break
                except:
                    del alternative_netlocs[netloc]
                
            if len(alternative_netlocs) == 0:
                raise MissingInputException(ref)
        
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
            ref.add_location_hint(parsed_url.netloc, ACCESS_SWBS)
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
            ref.add_location_hint(self.netloc, ACCESS_SWBS)
        
        return ref
        
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
        
    def block_list_generator(self):
        cherrypy.log.error('Generating block list for local consumption', 'BLOCKSTORE', logging.INFO)
        for block_name in os.listdir(self.base_dir):
            block_size = os.path.getsize(os.path.join(self.base_dir, block_name))
            yield block_name, block_size
    
    def generate_block_list_file(self):
        cherrypy.log.error('Generating block list file', 'BLOCKSTORE', logging.INFO)
        with tempfile.NamedTemporaryFile('w', delete=False) as block_list_file:
            filename = block_list_file.name
            for block_name in os.listdir(self.base_dir):
                block_id = block_name
                block_list_file.write(BLOCK_LIST_RECORD_STRUCT.pack(block_id, os.path.getsize(os.path.join(self.base_dir, block_id))))
        return filename

    def is_empty(self):
        return len(os.listdir(self.base_dir)) == 0
