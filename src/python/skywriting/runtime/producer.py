
import simplejson

# Maintains a set of block IDs that are currently being written.
# (i.e. They are in the pre-publish/streamable state, and have not been
#       committed to the block store.)
# They map to the executor which is producing them.
streaming_producers = dict()

# Remote endpoints that are receiving adverts from our streaming producers.
# Indexed by (refid, otherend_netloc)
remote_stream_subscribers = dict()

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
