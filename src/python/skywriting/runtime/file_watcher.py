
import threading
import os
import ciel
import logging

singleton_watcher = None

class FileWatcherThread:

    def __init__(self, bus, block_store):
        self.bus = bus
        self.block_store = block_store
        self.thread = threading.Thread(target=self.main_loop)
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.active_watches = set()
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

    def create_watch(self, output_ctx):
        return FileWatch(output_ctx, self)

    def add_watch(self, watch):
        with self.lock:
            self.active_watches.add(watch)
            self.condvar.notify_all()

    def remove_watch(self, watch):
        with self.lock:
            self.active_watches.discard(watch)
            self.condvar.notify_all()

    def main_loop(self):
        with self.lock:
            while True:
                dead_watches = []
                if self.should_stop:
                    return
                for watch in self.active_watches:
                    try:
                        watch.poll()
                    except Exception as e:
                        ciel.log("Watch died with exception %s: cancelled" % e, "FILE_WATCHER", logging.ERROR)
                        dead_watches.append(watch)
                for watch in dead_watches:
                    self.active_watches.discard(watch)
                self.condvar.wait(1)

class FileWatch:

    def __init__(self, output_ctx, thread):
        self.id = output_ctx.refid
        self.filename = thread.block_store.producer_filename(self.id)
        self.thread = thread
        self.output_ctx = output_ctx

    def poll(self):
        st = os.stat(self.filename)
        self.output_ctx.size_update(st.st_size)

    # Out-of-thread-call
    def start(self):
        self.thread.add_watch(self)

    # Out-of-thread call
    def cancel(self):
        self.thread.remove_watch(self)

    # Out-of-thread call
    def set_chunk_size(self, new_chunk_size):
        ciel.log("File-watch for %s: new chunk size %d. (ignored)" % (self.id, new_chunk_size), "FILE_WATCHER", logging.INFO)
        
def create_watcher_thread(bus, block_store):
    global singleton_watcher
    singleton_watcher = FileWatcherThread(bus, block_store)
    singleton_watcher.subscribe()

def get_watcher_thread():
    return singleton_watcher

def create_watch(output):
    return singleton_watcher.create_watch(output)
