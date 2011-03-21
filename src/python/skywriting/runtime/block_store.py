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
import threading
from threading import RLock, Lock
from skywriting.runtime.exceptions import \
    MissingInputException, RuntimeSkywritingError
import random
import subprocess
import urllib2
import shutil
import pickle
import os
import uuid
import struct
import tempfile
import logging
import re
import codecs
from datetime import datetime, timedelta
import time
from cStringIO import StringIO
from errno import EAGAIN, EPIPE
from cherrypy.process import plugins
from shared.io_helpers import MaybeFile
from skywriting.runtime.file_watcher import get_watcher_thread
from skywriting.runtime.pycurl_rpc import post_string_noreturn

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
import simplejson
from shared.references import SWRealReference,\
    build_reference_from_tuple, SW2_ConcreteReference, SWDataValue, encode_datavalue,\
    SWErrorReference, SW2_StreamReference,\
    SW2_TombstoneReference, SW2_FetchReference, SW2_FixedReference,\
    SW2_SweetheartReference, SW2_CompletedReference, SW2_SocketStreamReference
from skywriting.runtime.references import SWReferenceJSONEncoder
import hashlib
import contextlib
from skywriting.lang.parser import CloudScriptParser
import skywriting
import ciel
import socket
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

PIN_PREFIX = '.__pin__:'

length_regex = re.compile("^Content-Length:\s*([0-9]+)")
http_response_regex = re.compile("^HTTP/1.1 ([0-9]+)")

class StreamRetry:
    pass
STREAM_RETRY = StreamRetry()

singleton_blockstore = None

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

class BlockStore:

    def __init__(self, bus, hostname, port, base_dir, ignore_blocks=False):
        plugins.SimplePlugin.__init__(self, bus)
        self._lock = RLock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
        self.bus = bus
    
        self.pin_set = set()
    
        self.ignore_blocks = ignore_blocks

        self.encoders = {'noop': self.encode_noop, 'json': self.encode_json, 'pickle': self.encode_pickle}
        self.decoders = {'noop': self.decode_noop, 'json': self.decode_json, 'pickle': self.decode_pickle, 'handle': self.decode_handle, 'script': self.decode_script}

        global singleton_blockstore
        assert singleton_blockstore is None
        singleton_blockstore = self

    def start(self):
        self.file_watcher_thread = get_watcher_thread()

    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)

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
    
    def fetch_filename(self, id):
        return os.path.join(self.base_dir, '.fetch:%s' % id)
    
    def producer_filename(self, id):
        return os.path.join(self.base_dir, '.producer:%s' % id)
    
    def filename(self, id):
        return os.path.join(self.base_dir, str(id))

    def filename_for_ref(self, ref):
        if isinstance(ref, SW2_FixedReference):
            return os.path.join(self.base_dir, '.__fixed__.%s' % ref.id)
        else:
            return self.filename(ref.id)

    def commit_file(self, old_name, new_name):

        try:
            os.link(old_name, new_name)
        except OSError as e:
            if e.errno == 17: # File exists
                size_old = os.path.getsize(old_name)
                size_new = os.path.getsize(new_name)
                if size_old == size_new:
                    ciel.log('Produced/retrieved %s matching existing file (size %d): ignoring' % (new_name, size_new), 'BLOCKSTORE', logging.WARNING)
                else:
                    ciel.log('Produced/retrieved %s with size not matching existing block (old: %d, new %d)' % (new_name, size_old, size_new), 'BLOCKSTORE', logging.ERROR)
                    raise
            else:
                raise

    def commit_stream(self, id):
        ciel.log.error('Committing file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        with self._lock:
            del self.streaming_producers[id]
            self.commit_file(self.producer_filename(id), self.filename(id))

    def rollback_file(self, id):
        ciel.log.error('Rolling back streamed file for output %s' % id, 'BLOCKSTORE', logging.WARNING)
        with self._lock:
            del self.streaming_producers[id]

    def write_fixed_ref_string(self, string, fixed_ref):
        with open(self.filename_for_ref(fixed_ref), "w") as fp:
            fp.write(string)

    def ref_from_string(self, string, id):
        output_ctx = self.make_local_output(id)
        with open(output_ctx.get_filename(), "w") as fp:
            fp.write(string)
        output_ctx.close()
        return output_ctx.get_completed_ref()

    def cache_object(self, object, encoder, id):
        self.object_cache[(id, encoder)] = object        

    def ref_from_object(self, object, encoder, id):
        """Encodes an object, returning either a DataValue or ConcreteReference as appropriate"""
        self.cache_object(object, encoder, id)
        buffer = StringIO()
        self.encoders[encoder](object, buffer)
        ret = self.ref_from_string(buffer.getvalue(), id)
        buffer.close()
        return ret

    # Why not just rename to self.filename(id) and skip this nonsense? Because os.rename() can be non-atomic.
    # When it renames between filesystems it does a full copy; therefore I copy/rename to a colocated dot-file,
    # then complete the job by linking the proper name in output_ctx.close().
    def ref_from_external_file(self, filename, id):
        output_ctx = self.make_local_output(id)
        with output_ctx:
            shutil.move(filename, output_ctx.get_filename())
        return output_ctx.get_completed_ref()

    # Called from cURL thread
    def commit_fetch(self, ref):
        self.commit_file(self.fetch_filename(ref.id), self.filename(ref.id))

    def retrieve_objects_for_refs(self, ref_and_decoders):

        solutions = dict()
        unsolved_refs = []
        for (ref, decoder) in ref_and_decoders:
            try:
                solutions[ref.id] = self.object_cache[(ref.id, decoder)]
            except:
                unsolved_refs.append(ref)

        strings = self.retrieve_strings_for_refs(unsolved_refs)
        str_of_ref = dict([(ref.id, string) for (string, ref) in zip(strings, unsolved_refs)])
            
        for (ref, decoder) in ref_and_decoders:
            if ref.id not in solutions:
                decoded = self.decoders[decoder](StringIO(str_of_ref[ref.id]))
                self.object_cache[(ref.id, decoder)] = decoded
                solutions[ref.id] = decoded
            
        return [solutions[ref.id] for (ref, decoder) in ref_and_decoders]

    def retrieve_object_for_ref(self, ref, decoder):
        
        return self.retrieve_objects_for_refs([(ref, decoder)])[0]
                
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
            ref = self.ref_from_external_file(fetch_filename, id)

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

    def check_local_blocks(self):
        ciel.log("Looking for local blocks", "BLOCKSTORE", logging.INFO)
        try:
            for block_name in os.listdir(self.base_dir):
                if block_name.startswith('.fetch:'):
                    if not os.path.exists(os.path.join(self.base_dir, block_name[7:])):
                        ciel.log("Deleting incomplete block %s" % block_name, "BLOCKSTORE", logging.WARNING)
                        os.remove(os.path.join(self.base_dir, block_name))
                elif block_name.startswith('.producer:'):
                    if not os.path.exists(os.path.join(self.base_dir, block_name[10:])):
                        ciel.log("Deleting incomplete block %s" % block_name, "BLOCKSTORE", logging.WARNING)
                        os.remove(os.path.join(self.base_dir, block_name))                        
        except OSError as e:
            ciel.log("Couldn't enumerate existing blocks: %s" % e, "BLOCKSTORE", logging.WARNING)

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

### Stateless functions

def get_fetch_urls_for_ref(self, ref):

    if isinstance(ref, SW2_ConcreteReference):
        return ["http://%s/data/%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
    elif isinstance(ref, SW2_StreamReference):
        return ["http://%s/data/.producer:%s" % (loc_hint, ref.id) for loc_hint in ref.location_hints]
    elif isinstance(ref, SW2_FixedReference):
        assert ref.fixed_netloc == get_own_netloc()
        return ["http://%s/data/%s" % (ref.fixed_netloc, ref.id)]
    elif isinstance(ref, SW2_FetchReference):
        return [ref.url]

### Proxies against the singleton blockstore

def commit_fetch(ref):
    singleton_blockstore.commit_fetch(ref)

def get_own_netloc():
    return singleton_blockstore.netloc

def fetch_filename(id):
    return singleton_blockstore.fetch_filename(id)
    
def producer_filename(id):
    return singleton_blockstore.producer_filename(id)

def filename(id):
    return singleton_blockstore.filename(id)

def filename_for_ref(ref):
    return singleton_blockstore.filename_for_ref(ref)
