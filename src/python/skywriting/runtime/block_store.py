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
from cStringIO import StringIO

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
        elif isinstance(ref, SW2_ConcreteReference):
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

    def build_request_for_ref(self, (ref, resolution)):

        def build_request(check_urls, fetch_urls):
            return {"check_urls": check_urls, 
                    "failures": 0, 
                    "done": False,
                    "fetch_urls": fetch_urls,
                    "url": fetch_urls[0]}

        if resolution is not None:
            return {"done": True, "save_to": resolution, "succeeded": True}
        else:
            if isinstance(ref, SW2_ConcreteReference):
                check_urls = ["swbs://%s/%s" % (loc_hint, ref.id) for loc_hint in location_hints]
                fetch_urls = ["http://%s/data/%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
                return build_request(check_urls, fetch_urls)
            elif isinstance(ref, SWURLReference):
                fetch_urls = map(sw_to_external_url, ref.urls)
                return build_request(ref.urls, fetch_urls)
                
    def process_requests(self, reqs, do_store=True):

        # Try to fetch what we need, failing over to backup URLs as necessary.
        # This isn't the ideal algorithm, as it won't submit retry requests until all the current wave
        # has either succeeded or failed. That would require more pycURL understanding,
        # specifically how to add requests to an ongoing multi-request.

        live_requests = reqs
        while len(live_requests) > 0:
            live_requests = filter(lambda req : not req["done"], live_requests)
            grab_urls(live_requests)
            for req_result in live_requests:
                
                if req_result["succeeded"]:
                    req_result["done"] = True
                    if do_store:
                        self.store_url_in_cache(req_result["check_urls"][req_result["failures"]], req_result["save_to"])
                else:
                    try:
                        req_result["failures"] += 1
                        req_result["url"] = req_result["fetch_urls"][req_result["failures"]]
                        if "reset_callback" in req_result:
                            req_result["reset_callback"]()
                    except IndexError:
                        # Damn
                        req_result["done"] = True


    def retrieve_filenames_for_refs(self, refs, do_store=True):

        # Step 1: Resolve from local cache
        resolved_refs = map(self.try_retrieve_filename_for_ref_without_transfer, refs)

        # Step 2: Build request descriptors
        fetch_requests = map(self.build_request_for_ref, zip(refs, resolved_refs))

        for (ref, req) in zip(refs, fetch_requests):
            if not req["done"]:
                if isinstance(ref, SW2_ConcreteReference):
                    req["save_to"] = self.filename(ref.id)
                elif isinstance(ref, SWURLReference):
                    req["save_to"] = self.filename(self.allocate_new_id())

        self.process_requests(fetch_requests)

        def filename_of_req(req):
            if req["succeeded"]:
                return req["save_to"]
            else:
                return None

        filenames_to_return = map(filename_of_req, fetch_requests)
        return filenames_to_return

    def retrieve_objects_for_refs(self, refs, decoder):
        
        easy_solutions = [self.try_retrieve_object_for_ref_without_transfer(ref, decoder) for ref in refs]

        # The rest need a real fetch.
        request_descriptors = [self.build_request_for_ref(x) for x in zip(refs, easy_solutions)]

        def empty_buffer(req):
            req["buffer"].close()
            req["buffer"] = StringIO()

        for req in request_descriptors:
            if not req["done"]:
                req["buffer"] = StringIO()
                req["write_callback"] = req["buffer"].write
                req["reset_callback"] = empty_buffer

        self.process_requests(request_descriptors, False)

        def object_of_req(req):
            if "save_to" in req:
                # Object found in cache
                return req["save_to"]
            elif "buffer" in req and req["succeeded"]:
                # Object received from remote
                req["buffer"].seek(0)
                return self.decoders[decoder](req["buffer"])
            else:
                return None

        results = map(object_of_req, request_descriptors)

        for req in request_descriptors:
            if "buffer" in req:
                req["buffer"].close()

        return results

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
