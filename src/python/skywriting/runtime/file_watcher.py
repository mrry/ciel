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
        self.event_flag = False
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

    def add_watch(self, otherend_netloc, output_id):
        new_watch = FileWatch(otherend_netloc, output_id, self)
        with self.lock:
            self.active_watches.add(new_watch)
            self.event_flag = True
            self.condvar.notify_all()

    def main_loop(self):
        while True:
            with self.lock:
                if self.should_stop:
                    return
                if not self.event_flag:
                    self.condvar.wait(1)
                self.event_flag = False
                if self.should_stop:
                    return
                notifies = []
                for watch in self.active_watches:
                    if watch.should_notify():
                        notifies.append((watch, watch.netloc, watch.id, watch.size, watch.done))
                for (watch, _, _, _, done) in notifies:
                    if done:
                        self.active_watches.remove(watch)
            for (_, loc, id, size, done) in notifies:
                data = simplejson.dumps({"bytes": size, "done": done})
                httplib2.Http().request("http://%s/control/streamstat/%s/advert" % (loc, id), "POST", data)

class FileWatch:

    def __init__(self, netloc, id, thread):
        self.netloc = netloc
        self.id = id
        self.filename = thread.block_store.streaming_filename(id)
        self.thread = thread
        self.last_notify = None
        self.size = None
        self.done = False

    def should_notify(self):

        st = os.stat(self.filename)
        self.size = st.st_size
        # POLICY: Notify every 64MB
        should_notify = self.done or self.last_notify is None or self.size - self.last_notify > 67108864
        if should_notify:
            self.last_notify = self.size
        return should_notify

    # Out-of-thread call
    def file_done(self):
        with self.thread.lock:
            self.done = True
            self.thread.event_flag = True
            self.thread.condvar.notify_all()
        
def create_watcher_thread(bus, block_store):
    global singleton_watcher
    singleton_watcher = FileWatcherThread(bus, block_store)
    singleton_watcher.subscribe()

def get_watcher_thread():
    return singleton_watcher
