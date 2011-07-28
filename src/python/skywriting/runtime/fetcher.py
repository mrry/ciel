# Copyright (c) 2010--11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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

from shared.references import SW2_ConcreteReference, SW2_StreamReference, SW2_SocketStreamReference,\
    SWDataValue, SWErrorReference, SW2_FixedReference, decode_datavalue,\
    SW2_FetchReference, decode_datavalue_string, encode_datavalue,\
    SW2_TombstoneReference

import tempfile
import subprocess
import ciel
import logging
import threading
import os
from skywriting.runtime.pycurl_data_fetch import HttpTransferContext
from skywriting.runtime.tcp_data_fetch import TcpTransferContext
from skywriting.runtime.block_store import filename_for_ref, producer_filename,\
    get_own_netloc, create_datavalue_file
from skywriting.runtime.producer import get_producer_for_id,\
    ref_from_external_file, ref_from_string
from skywriting.runtime.exceptions import ErrorReferenceError,\
    MissingInputException
import hashlib
import contextlib
import urllib2
import urlparse

class AsyncPushThread:

    def __init__(self, ref, read_filename, write_filename, fetch_ip):
        self.ref = ref
        self.fetch_ip = fetch_ip
        self.next_threshold = fetch_ip.chunk_size
        self.success = None
        self.lock = threading.RLock()
        self.fetch_done = False
        self.stream_done = False
        self.stream_started = False
        self.bytes_copied = 0
        self.bytes_available = 0
        self.completed_ref = None
        self.condvar = threading.Condition(self.lock)
        self.thread = None
        self.read_filename = read_filename
        self.write_filename = write_filename

    def _check_completion(self):
        if self.success is False:
            ciel.log("Fetch for %s failed" % self.ref, "EXEC", logging.WARNING)
            return False
        elif self.success is True:
            ciel.log("Fetch for %s completed; using file directly" % self.ref, "EXEC", logging.DEBUG)
            return True
        else:
            return False

    def check_completion(self):
        ret = self._check_completion()
        if ret:
            self.fetch_ip.set_filename(self.read_filename, True)
        return ret

    def start(self):
        if not self.check_completion():
            self.thread = threading.Thread(target=self.thread_main)
            self.thread.start()

    def thread_main(self):

        with self.lock:
            if self.check_completion():
                return
            while self.bytes_available < self.next_threshold and not self.fetch_done:
                self.condvar.wait()
            if self.check_completion():
                return
            else:
                self.stream_started = True
        ciel.log("Fetch for %s got more than %d bytes; commencing asynchronous push" % (self.ref, self.fetch_ip.chunk_size), "EXEC", logging.DEBUG)

        self.copy_loop()

    def copy_loop(self):
        
        try:
            self.fetch_ip.set_filename(self.write_filename, True)
            with open(self.read_filename, "r") as input_fp:
                with open(self.write_filename, "w") as output_fp:
                    while True:
                        while True:
                            buf = input_fp.read(4096)
                            output_fp.write(buf)
                            self.bytes_copied += len(buf)
                            with self.lock:
                                if self.success is False or (self.bytes_copied == self.bytes_available and self.fetch_done):
                                    self.stream_done = True
                                    self.condvar.notify_all()
                                    ciel.log("FIFO-push for %s complete (success: %s)" % (self.ref, self.success), "EXEC", logging.DEBUG)
                                    return
                            if len(buf) < 4096:
                                # EOF, for now.
                                break
                        with self.lock:
                            self.next_threshold = self.bytes_copied + self.fetch_ip.chunk_size
                            while self.bytes_available < self.next_threshold and not self.fetch_done:
                                self.condvar.wait()
        except Exception as e:
            ciel.log("Push thread for %s died with exception %s" % (self.ref, e), "EXEC", logging.WARNING)
            with self.lock:
                self.stream_done = True
                self.condvar.notify_all()
                
    def result(self, success):
        with self.lock:
            if self.success is None:
                self.success = success
                self.fetch_done = True
                self.condvar.notify_all()
            # Else we've already failed due to a reset.

    def progress(self, bytes_downloaded):
        with self.lock:
            self.bytes_available = bytes_downloaded
            if self.bytes_available >= self.next_threshold:
                self.condvar.notify_all()

    def reset(self):
        ciel.log("Reset of streamed fetch for %s!" % self.ref, "EXEC", logging.WARNING)
        should_cancel = False
        with self.lock:
            if self.stream_started:
                should_cancel = True
        if should_cancel:
            ciel.log("FIFO-stream had begun: failing transfer", "EXEC", logging.ERROR)
            self.fetch_ip.cancel()

class PlanFailedError(Exception):
    pass

class FetchInProgress:

    def __init__(self, ref, result_callback, reset_callback, start_filename_callback, start_fd_callback, string_callback, progress_callback, chunk_size, may_pipe, sole_consumer, must_block, task_record):
        self.lock = threading.RLock()
        self.result_callback = result_callback
        self.reset_callback = reset_callback
        self.start_filename_callback = start_filename_callback
        self.start_fd_callback = start_fd_callback
        self.string_callback = string_callback
        self.progress_callback = progress_callback
        self.chunk_size = chunk_size
        self.may_pipe = may_pipe
        self.sole_consumer = sole_consumer
        self.must_block = must_block
        self.task_record = task_record
        self.pusher_thread = None
        self.ref = ref
        self.producer = None
        self.cat_process = None
        self.started = False
        self.done = False
        self.cancelled = False
        self.success = None
        self.form_plan()
        
    def form_plan(self):
        self.current_plan = 0
        self.plans = []
        if isinstance(self.ref, SWDataValue):
            self.plans.append(self.resolve_dataval)
        elif isinstance(self.ref, SW2_FetchReference):
            self.plans.append(self.http_fetch)
        else:
            self.plans.append(self.use_local_file)
            self.plans.append(self.attach_local_producer)
            if isinstance(self.ref, SW2_ConcreteReference):
                self.plans.append(self.http_fetch)
            elif isinstance(self.ref, SW2_StreamReference):
                if isinstance(self.ref, SW2_SocketStreamReference):
                    self.plans.append(self.tcp_fetch)
                self.plans.append(self.http_fetch)

    def start_fetch(self):
        self.run_plans()

    def run_plans(self):
        while self.current_plan < len(self.plans):
            try:
                self.plans[self.current_plan]()
                return
            except PlanFailedError:
                self.current_plan += 1

    def run_next_plan(self):
        self.current_plan += 1
        self.run_plans()

    def resolve_dataval(self):
        if self.string_callback is not None:
            decoded_dataval = decode_datavalue(self.ref)
            self.string_callback(decoded_dataval)
        else:
            create_datavalue_file(self.ref)
            self.set_filename(filename_for_ref(self.ref), True)
            self.result(True, None)

    def use_local_file(self):
        filename = filename_for_ref(self.ref)
        if os.path.exists(filename):
            self.set_filename(filename, True)
            self.result(True, None)
        else:
            raise PlanFailedError("Plan use-local-file failed for %s: no such file %s" % (self.ref, filename), "BLOCKSTORE", logging.DEBUG)

    def attach_local_producer(self):
        producer = get_producer_for_id(self.ref.id)
        if producer is None:
            raise PlanFailedError("Plan attach-local-producer failed for %s: not being produced here" % self.ref, "BLOCKSTORE", logging.DEBUG)
        else:
            is_pipe = producer.subscribe(self, try_direct=(self.may_pipe and self.sole_consumer))
            if is_pipe:
                ciel.log("Fetch-ref %s: attached to direct pipe!" % self.ref, "BLOCKSTORE", logging.DEBUG)
                filename = producer.get_fifo_filename()
            else:
                ciel.log("Fetch-ref %s: following local producer's file" % self.ref, "BLOCKSTORE", logging.DEBUG)
                filename = producer_filename(self.ref.id)
            self.set_filename(filename, is_pipe)

    def http_fetch(self):
        self.producer = HttpTransferContext(self.ref, self)
        self.producer.start()

    def tcp_fetch(self):
        if (not self.may_pipe) or (not self.sole_consumer):
            raise PlanFailedError("TCP-Fetch currently only capable of delivering a pipe")
        self.producer = TcpTransferContext(self.ref, self.chunk_size, self)
        self.producer.start()
                
    ### Start callbacks from above
    def result(self, success, result_ref=None):
        with self.lock:
            if not success:
                if not self.started:
                    self.run_next_plan()
                    return
            self.producer = None
            self.done = True
            self.success = success
        if self.pusher_thread is not None:
            self.pusher_thread.result(success)
        self.result_callback(success, result_ref)

    def reset(self):
        if self.pusher_thread is not None:
            self.pusher_thread.reset()
        self.reset_callback()

    def progress(self, bytes):
        if self.pusher_thread is not None:
            self.pusher_thread.progress(bytes)
        if self.progress_callback is not None:
            self.progress_callback(bytes)
            
    def create_fifo(self):
        fifo_name = tempfile.mktemp(prefix="ciel-socket-fifo")
        os.mkfifo(fifo_name)
        return fifo_name

    def set_fd(self, fd, is_pipe):
        # TODO: handle FDs that might point to regular files.
        assert is_pipe
        self.started = True
        if self.start_fd_callback is not None:
            self.start_fd_callback(fd, is_pipe)
        else:
            fifo_name = self.create_fifo()
            self.cat_process = subprocess.Popen(["cat > %s" % fifo_name], shell=True, stdin=fd, close_fds=True)
            os.close(fd)
            self.start_filename_callback(fifo_name, True)

    def set_filename(self, filename, is_pipe):
        self.started = True
        if (not is_pipe) and self.must_block:
            fifo_name = self.create_fifo()
            self.pusher_thread = AsyncPushThread(self.ref, filename, fifo_name, self)
            self.pusher_thread.start()
        else:
            self.start_filename_callback(filename, is_pipe)

    def cancel(self):
        with self.lock:
            self.cancelled = True
            producer = self.producer
        if producer is not None:
            producer.unsubscribe(self)
        if self.cat_process is not None:
            try:
                self.cat_process.kill()
            except Exception as e:
                ciel.log("Fetcher for %s failed to kill 'cat': %s" % (self.ref.id, repr(e)), "FETCHER", logging.ERROR)

# After you call this, you'll get some callbacks:
# 1. A start_filename or start_fd to announce that the transfer has begun and you can use the given filename or FD.
# 1a. Or, if the data was very short, perhaps a string-callback which concludes the transfer.
# 2. A series of progress callbacks to update you on how many bytes have been written
# 3. Perhaps a reset callback, indicating the transfer has rewound to the beginning.
# 4. A result callback, stating whether the transfer was successful, 
#    and if so, perhaps giving a reference to a local copy.
# Only the final result, reset and start-filename callbacks are non-optional:
# * If you omit start_fd_callback and a provider gives an FD, it will be cat'd into a FIFO 
#   and the name of that FIFO supplied.
# * If you omit string_callback and a provider supplies a string, it will be written to a file
# * If you omit progress_callback, you won't get progress notifications until the transfer is complete.
# Parameters:
# * may_pipe: allows a producer to supply data via a channel that blocks the producer until the consumer
#             has read sufficient data, e.g. a pipe or socket. Must be False if you intend to wait for completion.
# * sole_consumer: If False, a copy of the file will be made to local disk as well as being supplied to the consumer.
#                  If True, the file might be directly supplied to the consumer, likely dependent on may_pipe.
def fetch_ref_async(ref, result_callback, reset_callback, start_filename_callback, 
                    start_fd_callback=None, string_callback=None, progress_callback=None, 
                    chunk_size=67108864, may_pipe=False, sole_consumer=False,
                    must_block=False, task_record=None):

    if isinstance(ref, SWErrorReference):
        raise ErrorReferenceError(ref)
    if isinstance(ref, SW2_FixedReference):
        assert ref.fixed_netloc == get_own_netloc()

    new_client = FetchInProgress(ref, result_callback, reset_callback, 
                                 start_filename_callback, start_fd_callback, 
                                 string_callback, progress_callback, chunk_size,
                                 may_pipe, sole_consumer, must_block, task_record)
    new_client.start_fetch()
    return new_client

class SynchronousTransfer:
        
    def __init__(self, ref, task_record):
        self.ref = ref
        self.filename = None
        self.str = None
        self.success = None
        self.completed_ref = None
        self.task_record = task_record
        self.finished_event = threading.Event()

    def result(self, success, completed_ref):
        self.success = success
        self.completed_ref = completed_ref
        self.finished_event.set()

    def reset(self):
        pass

    def start_filename(self, filename, is_pipe):
        self.filename = filename

    def return_string(self, str):
        self.str = str
        self.success = True
        self.finished_event.set()

    def wait(self):
        self.finished_event.wait()
        
class FileOrString:
    
    def __init__(self, strdata=None, filename=None, completed_ref=None):
        self.str = strdata
        self.filename = filename
        self.completed_ref = completed_ref
            
    @staticmethod
    def from_dict(in_dict):
        return FileOrString(**in_dict)
    
    @staticmethod
    def from_safe_dict(in_dict):
        try:
            in_dict["strdata"] = decode_datavalue_string(in_dict["strdata"])
        except KeyError:
            pass
        return FileOrString(**in_dict)
    
    def to_dict(self):
        if self.str is not None:
            return {"strdata": self.str}
        else:
            return {"filename": self.filename}
        
    def to_safe_dict(self):
        if self.str is not None:
            return {"strdata": encode_datavalue(self.str)}
        else:
            return {"filename": self.filename}

    def to_ref(self, refid):
        if self.str is not None:
            ref = ref_from_string(self.str, refid)
        else:
            ref = ref_from_external_file(self.filename, refid)
        return ref

    def to_str(self):
        if self.str is not None:
            return self.str
        else:
            with open(self.filename, "r") as f:
                return f.read()
            
def sync_retrieve_refs(refs, task_record, accept_string=False):
    
    ctxs = []
    
    for ref in refs:
        sync_transfer = SynchronousTransfer(ref, task_record)
        ciel.log("Synchronous fetch ref %s" % ref.id, "BLOCKSTORE", logging.DEBUG)
        if accept_string:
            kwargs = {"string_callback": sync_transfer.return_string}
        else:
            kwargs = {}
        fetch_ref_async(ref, sync_transfer.result, sync_transfer.reset, sync_transfer.start_filename, task_record=task_record, **kwargs)
        ctxs.append(sync_transfer)
            
    for ctx in ctxs:
        ctx.wait()
            
    failed_transfers = filter(lambda x: not x.success, ctxs)
    if len(failed_transfers) > 0:
        raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints)) for ctx in failed_transfers]))
    return ctxs

def retrieve_files_or_strings_for_refs(refs, task_record):
    
    ctxs = sync_retrieve_refs(refs, task_record, accept_string=True)
    return [FileOrString(ctx.str, ctx.filename, ctx.completed_ref) for ctx in ctxs]

def retrieve_file_or_string_for_ref(ref, task_record):
    
    return retrieve_files_or_strings_for_refs([ref], task_record)[0]

def retrieve_filenames_for_refs(refs, task_record, return_ctx=False):
        
    ctxs = sync_retrieve_refs(refs, task_record, accept_string=False)
    if return_ctx:
        return [FileOrString(None, ctx.filename, ctx.completed_ref) for ctx in ctxs]
    else:
        return [x.filename for x in ctxs]

def retrieve_filename_for_ref(ref, task_record, return_ctx=False):

    return retrieve_filenames_for_refs([ref], task_record, return_ctx)[0]

def get_ref_for_url(url, version, task_id):
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
        ref = ref_from_external_file(fetch_filename, id)

    return ref

class OngoingFetch:

    def __init__(self, ref, chunk_size, task_record, sole_consumer=False, make_sweetheart=False, must_block=False, can_accept_fd=False):
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.bytes = 0
        self.ref = ref
        self.chunk_size = chunk_size
        self.sole_consumer = sole_consumer
        self.make_sweetheart = make_sweetheart
        self.task_record = task_record
        self.done = False
        self.success = None
        self.filename = None
        self.fd = None
        self.completed_ref = None
        self.file_blocking = None
        # may_pipe = True because this class is only used for async operations.
        # The only current danger of pipes is that waiting for a transfer to complete might deadlock.
        if can_accept_fd:
            fd_callback = self.set_fd
        else:
            fd_callback = None
        self.fetch_ctx = fetch_ref_async(ref, 
                                         result_callback=self.result,
                                         progress_callback=self.progress, 
                                         reset_callback=self.reset,
                                         start_filename_callback=self.set_filename,
                                         start_fd_callback=fd_callback,
                                         chunk_size=chunk_size,
                                         may_pipe=True,
                                         must_block=must_block,
                                         sole_consumer=sole_consumer,
                                         task_record=task_record)
        
    def progress(self, bytes):
        with self.lock:
            self.bytes = bytes
            self.condvar.notify_all()

    def result(self, success, completed_ref):
        with self.lock:
            self.done = True
            self.success = success
            self.completed_ref = completed_ref
            self.condvar.notify_all()

    def reset(self):
        with self.lock:
            self.done = True
            self.success = False
            self.condvar.notify_all()
        # XXX: This is causing failures. Is it a vestige?
        #self.client.cancel()

    def set_filename(self, filename, is_blocking):
        with self.lock:
            self.filename = filename
            self.file_blocking = is_blocking
            self.condvar.notify_all()
            
    def set_fd(self, fd, is_blocking):
        with self.lock:
            self.fd = fd
            self.file_blocking = is_blocking
            self.condvar.notify_all()

    def get_filename(self):
        with self.lock:
            while self.filename is None and self.success is not False:
                self.condvar.wait()
            if self.filename is not None:
                return (self.filename, self.file_blocking)
            else:
                return (None, None)
        
    def get_fd(self):
        with self.lock:
            while self.fd is None and self.filename is None and self.success is not False:
                self.condvar.wait()
            if self.fd is not None:
                return (self.fd, self.file_blocking)
            else:
                return (None, None)

    def get_completed_ref(self):
        return self.completed_ref

    def wait_bytes(self, bytes):
        with self.lock:
            while self.bytes < bytes and not self.done:
                self.condvar.wait()

    def wait_eof(self):
        with self.lock:
            while not self.done:
                self.condvar.wait()

    def cancel(self):
        self.fetch_ctx.cancel()

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if not self.done:
            ciel.log("Cancelling async fetch for %s" % self.ref, "EXEC", logging.WARNING)
            self.cancel()
        return False

def retrieve_strings_for_refs(refs, task_record):

    ctxs = retrieve_files_or_strings_for_refs(refs, task_record)
    return [ctx.to_str() for ctx in ctxs]

def retrieve_string_for_ref(ref, task_record):
        
    return retrieve_strings_for_refs([ref], task_record)[0]
