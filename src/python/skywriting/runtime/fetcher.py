
from shared.references import SW2_ConcreteReference, SW2_StreamReference, SW2_SocketStreamReference, SWDataValue, SWErrorReference, SW2_FixedReference, decode_datavalue

import ciel
import threading
from skywriting.runtime.pycurl_data_fetch import HttpTransferContext
from skywriting.runtime.tcp_data_fetch import TcpTransferContext
from skywriting.runtime.block_store import filename_for_ref

class FetchInProgress:

    def __init__(self, ref, result_callback, reset_callback, start_filename_callback, start_fd_callback, string_callback, progress_callback, chunk_size, may_pipe, sole_consumer):
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
        self.ref = ref
        self.producer = None
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
            except Exception as e:
                self.current_plan += 1

    def run_next_plan(self):
        self.current_plan += 1
        self.run_plans()

    def resolve_dataval(self):
        decoded_dataval = decode_datavalue(ref)
        if self.string_callback is not None:
            self.string_callback(decoded_datavalue)
        else:
            filename = fetch_filename(ref)
            with open(filename, 'w') as obj_file:
                obj_file.write(decoded_dataval)
            commit_fetch(ref)
            self.set_filename(filename_for_ref(ref), True)
            self.result(True, None)

    def use_local_file(self):
        filename = filename_for_ref(ref)
        if os.path.exists(filename):
            self.set_filename(filename, True)
            self.result(True, None)
        else:
            raise Exception("Plan use-local-file failed for %s: no such file %s" % (self.ref, filename), "BLOCKSTORE", logging.INFO)

    def attach_local_producer(self):
        with self.lock:
            if ref.id in self.block_store.streaming_producers:
                ciel.log("Ref %s is being produced locally! Joining..." % ref, "BLOCKSTORE", logging.INFO)
                self.producer = self.block_store.streaming_producers[ref.id]
                filename, is_pipe = self.producer.subscribe(self, self.may_pipe)
                if is_pipe:
                    ciel.log("Fetch-ref %s: attached to direct pipe!" % ref, "BLOCKSTORE", logging.INFO)
                else:
                    ciel.log("Fetch-ref %s: following producer's file" % ref, "BLOCKSTORE", logging.INFO)
                self.set_filename(filename, is_pipe)
            else:
                raise Exception("Plan attach-local-producer failed for %s: not being produced here" % self.ref, "BLOCKSTORE", logging.INFO)

    def http_fetch(self):
        self.producer = HttpTransferContext(self.ref, self)
        self.producer.start()

    def tcp_fetch(self):
        if (not self.may_pipe) or (not self.sole_consumer):
            raise Exception("TCP-Fetch currently only capable of delivering a pipe")
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
        self.result_callback(success, result_ref)

    def reset(self):
        self.reset_callback()

    def progress(self, bytes):
        if self.progress_callback is not None:
            self.progress_callback(bytes)

    def set_fd(self, fd, is_pipe):
        # TODO: handle FDs that might point to regular files.
        assert is_pipe
        self.started = True
        if self.start_fd_callback is not None:
            self.start_fd_callback(fd, is_pipe)
        else:
            fifo_name = tempfile.mktemp(prefix="ciel-socket-fifo")
            os.mkfifo(fifo_name)
            subprocess.Popen(["cat > %s" % fifo_name], shell=True, stdin=fd, close_fds=True)
            os.close(fd)
            self.start_filename_callback(fifo_name, True)

    def set_filename(self, filename, is_pipe):
        self.started = True
        self.start_filename_callback(filename, is_pipe)

    def cancel(self):
        with self.lock:
            self.cancelled = True
            producer = self.producer
        if producer is not None:
            producer.unsubscribe(self)

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
#                  If True, the file might be directly supplied to the producer, likely dependent on may_pipe.
def fetch_ref_async(self, ref, result_callback, reset_callback, start_filename_callback, 
                    start_fd_callback=None, string_callback=None, progress_callback=None, 
                    chunk_size=67108864, may_pipe=False, sole_consumer=False):

    if isinstance(ref, SWErrorReference):
        raise RuntimeSkywritingError()
    if isinstance(ref, SW2_FixedReference):
        assert ref.fixed_netloc == self.netloc

    new_client = BlockStore.FetchInProgress(ref, result_callback, reset_callback, 
                                            start_filename_callback, start_fd_callback, 
                                            string_callback, progress_callback, chunk_size,
                                            may_pipe, sole_consumer)
    new_client.start_fetch()
    return new_client

class SynchronousTransfer:
        
    def __init__(self, ref):
        self.ref = ref
        self.finished_event = threading.Event()

    def result(self, success):
        self.success = success
        self.finished_event.set()

    def reset(self):
        pass

    def start_filename(self, filename, is_pipe):
        self.filename = filename

    def wait(self):
        self.finished_event.wait()

def retrieve_filenames_for_refs(self, refs):
        
    ctxs = []
    for ref in refs:
        sync_transfer = BlockStore.SynchronousTransfer(ref)
        ciel.log("Synchronous fetch ref %s" % ref, "BLOCKSTORE", logging.INFO)
        transfer_ctx = self.fetch_ref_async(ref, sync_transfer.result, sync_transfer.reset, sync_transfer.start_filename)
        ctxs.append(sync_transfer)
            
    for ctx in ctxs:
        ctx.wait()
            
    failed_transfers = filter(lambda x: not x.success, ctxs)
    if len(failed_transfers) > 0:
        raise MissingInputException(dict([(ctx.ref.id, SW2_TombstoneReference(ctx.ref.id, ctx.ref.location_hints)) for ctx in failed_transfers]))
    return [x.filename for x in sync_transfers]

def retrieve_filename_for_ref(self, ref):

    return self.retrieve_filenames_for_refs([ref])[0]

def retrieve_strings_for_refs(self, refs):

    strs = []
    files = self.retrieve_filenames_for_refs(refs)
    for fname in files:
        with open(fname, "r") as fp:
            strs.append(fp.read())
    return strs

def retrieve_string_for_ref(self, ref):
        
    return self.retrieve_strings_for_refs([ref])[0]
