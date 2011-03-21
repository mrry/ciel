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

    def __init__(self, bus, hostname, port, base_dir, ignore_blocks=False, aux_listen_port=None):
        plugins.SimplePlugin.__init__(self, bus)
        self._lock = RLock()
        self.netloc = "%s:%s" % (hostname, port)
        self.base_dir = base_dir
        self.object_cache = {}
        self.bus = bus
        self.aux_listen_port = aux_listen_port
    
        self.pin_set = set()
    
        self.ignore_blocks = ignore_blocks
    
        # Maintains a set of block IDs that are currently being written.
        # (i.e. They are in the pre-publish/streamable state, and have not been
        #       committed to the block store.)
        # They map to the executor which is producing them.
        self.streaming_producers = dict()

        # Remote endpoints that are receiving adverts from our streaming producers.
        # Indexed by (refid, otherend_netloc)
        self.remote_stream_subscribers = dict()

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
        
    class RemoteOutputSubscriber:
        
        def __init__(self, file_output, netloc, chunk_size):
            self.file_output = file_output
            self.netloc = netloc
            self.chunk_size = chunk_size
            self.current_size = None
            self.last_notify = None

        def set_chunk_size(self, chunk_size):
            self.chunk_size = chunk_size
            if self.current_size is not None:
                self.post(simplejson.dumps({"bytes": self.current_size, "done": False}))
            self.file_output.chunk_size_changed(self)

        def unsubscribe(self):
            self.file_output.unsubscribe(self)

        def post(self, message):
            post_string_noreturn("http://%s/control/streamstat/%s/advert" % (self.netloc, self.file_output.refid), message)

        def progress(self, bytes):
            self.current_size = bytes
            if self.last_notify is None or self.current_size - self.last_notify > self.chunk_size:
                data = simplejson.dumps({"bytes": bytes, "done": False})
                self.post(data)
                self.last_notify = self.current_size

        def result(self, success):
            if success:
                self.post(simplejson.dumps({"bytes": self.current_size, "done": True}))
            else:
                self.post(simplejson.dumps({"failed": True}))

        
    class FileOutputContext:

        # may_pipe: Should wait for a direct connection, either via local pipe or direct remote socket
        # single_consumer: (required for may-pipe): this output has one consumer. We'll allow a socket consumer.
        #   XXX: This isn't the right place to do this; we're saving the output locally anyway. This is just to prevent multiple socket-fetches of the same thing.
        def __init__(self, refid, block_store, subscribe_callback, single_consumer, may_pipe):
            self.refid = refid
            self.block_store = block_store
            self.subscribe_callback = subscribe_callback
            self.file_watch = None
            self.subscriptions = []
            self.current_size = None
            self.closed = False
            self.single_consumer = single_consumer
            self.may_pipe = may_pipe
            if self.may_pipe and not self.single_consumer:
                raise Exception("invalid option combination: using pipes but not single-consumer")
            if self.may_pipe:
                self.fifo_name = tempfile.mktemp()
                os.mkfifo(self.fifo_name)
                self.pipe_deadline = datetime.now() + timedelta(seconds=5)
                self.started = False
                self.pipe_attached = False
                self.cond = threading.Condition(self.block_store._lock)

        def get_filename(self):
            # XXX: Consumers actually call this function too! They call try_get_pipe / subscribe first though, so they never end up waiting like a producer.
            # To fix after paper deadline.
            if self.may_pipe:
                with self.block_store._lock:
                    if not self.pipe_attached:
                        now = datetime.now()
                        if now < self.pipe_deadline:
                            wait_time = self.pipe_deadline - now
                            wait_secs = float(wait_time.seconds) + (float(wait_time.microseconds) / 10**6)
                            ciel.log("Producer for %s: waiting for pipe pickup" % self.refid, "BLOCKPIPE", logging.INFO)
                            self.cond.wait(wait_secs)
                    if self.pipe_attached:
                        ciel.log("Producer for %s: using pipe" % self.refid, "BLOCKPIPE", logging.INFO)
                        self.started = True
                        return self.fifo_name
                    elif self.started:
                        ciel.log("Producer for %s: kicked by a regular-file subscription; using conventional stream-file" % self.refid, "BLOCKPIPE", logging.INFO)
                    else:
                        self.started = True
                        ciel.log("Producer for %s: timed out waiting for a consumer; using conventional stream-file" % self.refid, "BLOCKPIPE", logging.INFO)
            return self.block_store.producer_filename(self.refid)

        def get_stream_ref(self):
            if self.single_consumer and self.block_store.aux_listen_port is not None:
                return SW2_SocketStreamReference(self.refid, self.block_store.netloc, self.block_store.aux_listen_port)
            else:
                return SW2_StreamReference(self.refid, location_hints=[self.block_store.netloc])

        def rollback(self):
            if not self.closed:
                self.closed = True
                self.block_store.rollback_file(self.refid)
                if self.file_watch is not None:
                    self.file_watch.cancel()
                for subscriber in self.subscriptions:
                    subscriber.result(False)

        def close(self):
            if not self.closed:
                self.closed = True
                self.block_store.commit_stream(self.refid)
                # At this point no subscribe() calls are in progress.
                if self.file_watch is not None:
                    self.file_watch.cancel()
                self.current_size = os.stat(self.block_store.filename(self.refid)).st_size
                for subscriber in self.subscriptions:
                    subscriber.progress(self.current_size)
                    subscriber.result(True)

        def get_completed_ref(self):
            if not self.closed:
                raise Exception("FileOutputContext for ref %s must be closed before it is realised as a concrete reference" % self.refid)
            if self.may_pipe and self.pipe_attached:
                return SW2_CompletedReference(self.refid)
            completed_file = self.block_store.filename(self.refid)
            if self.current_size < 1024:
                with open(completed_file, "r") as fp:
                    return SWDataValue(self.refid, encode_datavalue(fp.read()))
            else:
                return SW2_ConcreteReference(self.refid, size_hint=self.current_size, location_hints=[self.block_store.netloc])

        def update_chunk_size(self):
            self.subscriptions.sort(key=lambda x: x.chunk_size)
            self.file_watch.set_chunk_size(self.subscriptions[0].chunk_size)

        def try_get_pipe(self):
            if not self.may_pipe:
                return None
            else:
                with self.block_store._lock:
                    if self.started:
                        ciel.log("Consumer for %s: production already started, not using pipe" % self.refid, "BLOCKPIPE", logging.INFO)
                        return None
                    ciel.log("Consumer for %s: attached to local pipe" % self.refid, "BLOCKPIPE", logging.INFO)
                    self.pipe_attached = True
                    self.cond.notify_all()
                    return self.fifo_name

        def subscribe(self, new_subscriber):

            if self.may_pipe:
                with self.block_store._lock:
                    if self.pipe_attached:
                        raise Exception("Tried to subscribe to output %s, but it's already being consumed through a pipe! Bug? Or duplicate consumer task?" % self.refid)
                    self.started = True
                    self.cond.notify_all()
            should_start_watch = False
            self.subscriptions.append(new_subscriber)
            if self.current_size is not None:
                new_subscriber.progress(self.current_size)
            if self.file_watch is None:
                ciel.log("Starting watch on output %s" % self.refid, "BLOCKSTORE", logging.INFO)
                self.file_watch = self.subscribe_callback(self)
                should_start_watch = True
            self.update_chunk_size()
            if should_start_watch:
                self.file_watch.start()

        def unsubscribe(self, subscriber):
            try:
                self.subscriptions.remove(subscriber)
            except ValueError:
                ciel.log("Couldn't unsubscribe %s from output %s: not a subscriber" % (subscriber, self.refid), "BLOCKSTORE", logging.ERROR)
            if len(self.subscriptions) == 0 and self.file_watch is not None:
                ciel.log("No more subscribers for %s; cancelling watch" % self.refid, "BLOCKSTORE", logging.INFO)
                self.file_watch.cancel()
                self.file_watch = None
            else:
                self.update_chunk_size()

        def chunk_size_changed(self, subscriber):
            self.update_chunk_size()

        def size_update(self, new_size):
            self.current_size = new_size
            for subscriber in self.subscriptions:
                subscriber.progress(new_size)

        def __enter__(self):
            return self

        def __exit__(self, exnt, exnv, exntb):
            if not self.closed:
                if exnt is None:
                    self.close()
                else:
                    ciel.log("FileOutputContext %s destroyed due to exception %s: rolling back" % (self.refid, repr(exnv)), "BLOCKSTORE", logging.WARNING)
                    self.rollback()
            return False

    def register_local_output(self, id, new_producer):
        with self._lock:
            self.streaming_producers[id] = new_producer
            dot_filename = self.producer_filename(id)
            open(dot_filename, 'wb').close()
            return

    def make_local_output(self, id, subscribe_callback=None, single_consumer=False, may_pipe=False):
        '''
        Creates a file-in-progress in the block store directory.
        '''
        if subscribe_callback is None:
            subscribe_callback = self.create_file_watch
        ciel.log.error('Creating file for output %s' % id, 'BLOCKSTORE', logging.INFO)
        new_ctx = BlockStore.FileOutputContext(id, self, subscribe_callback, single_consumer, may_pipe)
        self.register_local_output(id, new_ctx)
        return new_ctx

    def create_file_watch(self, output_ctx):
        return self.file_watcher_thread.create_watch(output_ctx)

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

    # This is a lot like the AsyncPushThread in executors.py.
    # TODO after paper rush is over: get the spaghettificiation of the streaming code under control

    class SocketPusher:
        
        def __init__(self, refid, sock_obj, chunk_size):
            self.refid = refid
            self.sock_obj = sock_obj
            self.bytes_available = 0
            self.bytes_copied = 0
            self.fetch_done = False
            self.pause_threshold = None
            self.chunk_size = chunk_size
            self.lock = threading.Lock()
            self.cond = threading.Condition(self.lock)
            self.thread = threading.Thread(target=self.thread_main)
        
        def result(self, success):
            if not success:
                raise Exception("No way to signal failure to TCP consumers yet!")
            with self.lock:
                self.fetch_done = True
                self.cond.notify_all()

        def progress(self, n_bytes):
            with self.lock:
                self.bytes_available = n_bytes
                if self.pause_threshold is not None and self.bytes_available >= self.pause_threshold:
                    self.cond.notify_all()

        def set_filename(self, filename):
            self.read_filename = filename

        def start(self):
            self.thread.start()

        def thread_main(self):
            try:
                with open(self.read_filename, "r") as input_fp:
                    while True:
                        while True:
                            buf = input_fp.read(4096)
                            self.sock_obj.sendall(buf)
                            self.bytes_copied += len(buf)
                            with self.lock:
                                if self.bytes_copied == self.bytes_available and self.fetch_done:
                                    ciel.log("Socket-push for %s complete: wrote %d bytes" % (self.refid, self.bytes_copied), "EXEC", logging.INFO)
                                    self.sock_obj.close()
                                    return
                                if len(buf) < self.chunk_size:
                                    # EOF, for now.
                                    break
                        with self.lock:
                            self.pause_threshold = self.bytes_copied + self.chunk_size
                            while self.bytes_available < self.pause_threshold and not self.fetch_done:
                                self.cond.wait()
                            self.pause_threshold = None

            except Exception as e:
                ciel.log("Socket-push-thread died with exception %s" % repr(e), "TCP_FETCH", logging.ERROR)
                try:
                    self.sock_obj.close()
                except:
                    pass
                                        
    def new_aux_connection(self, new_sock):
        try:
            new_sock.setblocking(True)
            sock_file = new_sock.makefile("r", bufsize=0)
            bits = sock_file.readline().strip().split()
            output_id = bits[0]
            chunk_size = bits[1]
            sock_file.close()
            with self._lock:
                try:
                    producer = self.streaming_producers[output_id]
                except KeyError:
                    ciel.log("Got auxiliary TCP connection for bad output %s" % output_id, "TCP_FETCH", logging.WARNING)
                    new_sock.sendall("FAIL\n")
                    new_sock.close()
                    return
                fifo_name = producer.try_get_pipe()
                if fifo_name is None:
                    sock_pusher = BlockStore.SocketPusher(output_id, new_sock, int(chunk_size))
                    producer.subscribe(sock_pusher)
                    sock_pusher.set_filename(producer.get_filename())
                    new_sock.sendall("GO\n")
                    sock_pusher.start()
                    ciel.log("Auxiliary TCP connection for output %s (chunk %s) attached via push thread" % (output_id, chunk_size), "TCP_FETCH", logging.INFO)
                else:
                    new_sock.sendall("GO\n")
                    ciel.log("Auxiliary TCP connection for output %s (chunk %s) attached direct to process; starting 'cat'" % (output_id, chunk_size), "TCP_FETCH", logging.INFO)
                    subprocess.Popen(["cat < %s" % fifo_name], shell=True, stdout=new_sock.fileno(), close_fds=True)
                    new_sock.close()
        except Exception as e:
            ciel.log("Error handling auxiliary TCP connection: %s" % repr(e), "TCP_FETCH", logging.ERROR)
            try:
                new_sock.close()
            except:
                pass

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

    # Remote is subscribing to updates from one of our streaming producers
    def subscribe_to_stream(self, otherend_netloc, chunk_size, id):
        post = None
        with self._lock:
            try:
                producer = self.streaming_producers[id]
                try:
                    self.remote_stream_subscribers[(id, otherend_netloc)].set_chunk_size(chunk_size)
                    ciel.log("Remote %s changed chunk size for %s to %d" % (otherend_netloc, id, chunk_size), "BLOCKSTORE", logging.INFO)
                except KeyError:
                    new_subscriber = BlockStore.RemoteOutputSubscriber(producer, otherend_netloc, chunk_size)
                    producer.subscribe(new_subscriber)
                    ciel.log("Remote %s subscribed to output %s (chunk size %d)" % (otherend_netloc, id, chunk_size), "BLOCKSTORE", logging.INFO)
            except KeyError:
                try:
                    st = os.stat(self.filename(id))
                    post = simplejson.dumps({"bytes": st.st_size, "done": True})
                except OSError:
                    post = simplejson.dumps({"absent": True})
            except Exception as e:
                ciel.log("Subscription to %s failed with exception %s; reporting absent" % (id, e), "BLOCKSTORE", logging.WARNING)
                post = simplejson.dumps({"absent": True})
        if post is not None:
            post_string_noreturn("http://%s/control/streamstat/%s/advert" % (otherend_netloc, id), post)

    def unsubscribe_from_stream(self, otherend_netloc, id):
        with self._lock:
            try:
                self.remote_stream_subscribers[(id, otherend_netloc)].cancel()
                ciel.log("%s unsubscribed from %s" % (otherend_netloc, id), "BLOCKSTORE", logging.INFO)
            except KeyError:
                ciel.log("Ignored unsubscribe request for unknown block %s" % id, "BLOCKSTORE", logging.WARNING)

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
