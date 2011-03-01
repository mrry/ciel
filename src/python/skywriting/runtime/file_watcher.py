import httplib2
import simplejson
import threading
import os
import ciel

singleton_watcher = None

class FileWatcherThread:

    def __init__(self, bus, block_store):
        self.bus = bus
        self.block_store = block_store
        self.thread = threading.Thread(target=self.main_loop)
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.active_watches = set()
        self.new_watches = []
        self.dead_watches = []
        self.should_stop = False

    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop, 10)

    def start(self):
        self.thread.start()

    def stop(self):
        with self.lock:
            self.should_stop = True
            self.condvar.notify_all()

    def add_watch(self, output_ctx):
        new_watch = FileWatch(output_ctx, self)
        with self.lock:
            self.new_watches.append(new_watch)
            self.condvar.notify_all()
        return new_watch

    def remove_watch(self, watch):
        with self.lock:
            self.dead_watches.append(self)
            self.condvar.notify_all()
            while watch in self.dead_watches:
                self.condvar.wait()

    def main_loop(self):
        while True:
            if self.should_stop:
                return
            for watch in self.active_watches:
                try:
                    watch.poll():
                except Exception as e:
                    ciel.log("Watch died with exception %s: cancelled" % e, "FILE_WATCHER", logging.ERROR)
                    self.dead_watches.append(watch)
            with self.lock:
                if len(self.new_watches) == 0 and len(self.dead_watches) == 0:
                    self.condvar.wait(1)
                for watch in self.new_watches:
                    self.active_watches.add(watch)
                self.new_watches = []
                for watch in self.dead_watches:
                    self.active_watches.remove(watch)
                self.dead_watches = []

class FileWatch:

    def __init__(self, output_ctx, thread):
        self.id = id
        self.filename = thread.block_store.streaming_filename(id)
        self.thread = thread
        self.done = False
        self.output_ctx = output_ctx

    def poll(self):
        st = os.stat(self.filename)
        self.output_ctx.size_update(st.st_size)

    # Out-of-thread call
    def cancel(self):
        self.thread.remove_watch(self)
        
def create_watcher_thread(bus, block_store):
    global singleton_watcher
    singleton_watcher = FileWatcherThread(bus, block_store)
    singleton_watcher.subscribe()

def get_watcher_thread():
    return singleton_watcher
