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
import random
import os
import uuid
import struct
import tempfile
import logging
import re
import threading
from datetime import datetime

# XXX: Hack because urlparse doesn't nicely support custom schemes.
import urlparse
from shared.references import SW2_ConcreteReference, SW2_StreamReference,\
    SW2_FetchReference, SW2_FixedReference, SWRealReference, SWErrorReference,\
    SWDataValue, decode_datavalue
import ciel
from skywriting.runtime.exceptions import RuntimeSkywritingError
urlparse.uses_netloc.append("swbs")

BLOCK_LIST_RECORD_STRUCT = struct.Struct("!120pQ")

PIN_PREFIX = '.__pin__:'

length_regex = re.compile("^Content-Length:\s*([0-9]+)")
http_response_regex = re.compile("^HTTP/1.1 ([0-9]+)")

singleton_blockstore = None

def get_netloc_for_sw_url(url):
    return urlparse.urlparse(url).netloc

def get_id_for_sw_url(url):
    return urlparse.urlparse(url).path

def sw_to_external_url(url):
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme == 'swbs':
        id = parsed_url.path[1:]
        return 'http://%s/data/%s' % (parsed_url.netloc, id)
    else:
        return url

class BlockStore:

    def __init__(self, hostname, port, base_dir, ignore_blocks=False):
        self.hostname = None
        self.port = port
        self.netloc = None
        if hostname is not None:
            self.set_hostname(hostname)
        # N.B. This is not set up for workers until contact is made with the master.

        self.base_dir = base_dir
        self.pin_set = set()
        self.ignore_blocks = ignore_blocks
        self.lock = threading.Lock()

        global singleton_blockstore
        assert singleton_blockstore is None
        singleton_blockstore = self

    def set_hostname(self, hostname):
        self.hostname = hostname
        self.netloc = '%s:%d' % (self.hostname, self.port)

    def allocate_new_id(self):
        return str(uuid.uuid1())
    
    def pin_filename(self, id): 
        return os.path.join(self.base_dir, PIN_PREFIX + id)
    
    class OngoingFetch:

        def __init__(self, ref, block_store):
            self.ref = ref
            self.filename = None
            self.block_store = block_store
            while self.filename is None:
                possible_name = os.path.join(block_store.base_dir, ".fetch:%s:%s" % (datetime.now().microsecond, ref.id))
                if not os.path.exists(possible_name):
                    self.filename = possible_name

        def commit(self):
            self.block_store.commit_file(self.filename, self.block_store.filename_for_ref(self.ref))

    def create_fetch_file_for_ref(self, ref):
        with self.lock:
            return BlockStore.OngoingFetch(ref, self)
    
    def producer_filename(self, id):
        return os.path.join(self.base_dir, '.producer:%s' % id)
    
    def filename(self, id):
        return os.path.join(self.base_dir, str(id))

    def filename_for_ref(self, ref):
        if isinstance(ref, SW2_FixedReference):
            return os.path.join(self.base_dir, '.__fixed__.%s' % ref.id)
        else:
            return self.filename(ref.id)
        
    def is_ref_local(self, ref):
        assert isinstance(ref, SWRealReference)

        if isinstance(ref, SWErrorReference):
            raise RuntimeSkywritingError()

        if isinstance(ref, SW2_FixedReference):
            assert ref.fixed_netloc == self.netloc
            
        if os.path.exists(self.filename_for_ref(ref)):
            return True
        if isinstance(ref, SWDataValue):
            create_datavalue_file(ref)
            return True

        return False

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

    def commit_producer(self, id):
        ciel.log.error('Committing file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        self.commit_file(self.producer_filename(id), self.filename(id))
        
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

def get_fetch_urls_for_ref(ref):

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

def commit_producer(id):
    singleton_blockstore.commit_producer(id)

def get_own_netloc():
    return singleton_blockstore.netloc

def create_fetch_file_for_ref(ref):
    return singleton_blockstore.create_fetch_file_for_ref(ref)
    
def producer_filename(id):
    return singleton_blockstore.producer_filename(id)

def filename(id):
    return singleton_blockstore.filename(id)

def filename_for_ref(ref):
    return singleton_blockstore.filename_for_ref(ref)

def is_ref_local(ref):
    return singleton_blockstore.is_ref_local(ref)

def create_datavalue_file(ref):
    bs_ctx = create_fetch_file_for_ref(ref)
    with open(bs_ctx.filename, 'w') as obj_file:
        obj_file.write(decode_datavalue(ref))
    bs_ctx.commit()

