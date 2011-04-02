
import ciel
import logging
import fcntl
import pycurl
import os
import threading
import select

from errno import EAGAIN

class pycURLContext:

    def __init__(self, url, result_callback):

        self.result_callback = result_callback
        self.url = url

        self.curl_ctx = pycurl.Curl()
        self.curl_ctx.setopt(pycurl.FOLLOWLOCATION, 1)
        self.curl_ctx.setopt(pycurl.MAXREDIRS, 5)
        self.curl_ctx.setopt(pycurl.CONNECTTIMEOUT, 30)
        self.curl_ctx.setopt(pycurl.TIMEOUT, 300)
        self.curl_ctx.setopt(pycurl.NOSIGNAL, 1)
        self.curl_ctx.setopt(pycurl.URL, str(url))

        self.curl_ctx.ctx = self

    def start(self):
        add_fetch(self)

    def success(self):
        self.result_callback(True)
        self.cleanup()

    def failure(self, errno, errmsg):
        ciel.log("Transfer failure: %s error %s / %s" % (self.url, errno, errmsg), "CURL", logging.WARNING)
        self.result_callback(False)
        self.cleanup()

    def cancel(self):
        remove_fetch(self)

    def cleanup(self):
        self.curl_ctx.close()

class SelectableEventQueue:

    def set_fd_nonblocking(self, fd):
        oldflags = fcntl.fcntl(fd, fcntl.F_GETFL)
        newflags = oldflags | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, newflags)

    def __init__(self):
        self._lock = threading.Lock()
        self.event_pipe_read, self.event_pipe_write = os.pipe()
        self.set_fd_nonblocking(self.event_pipe_read)
        self.set_fd_nonblocking(self.event_pipe_write)
        self.event_queue = []

    def drain_event_pipe(self):
        try:
            while(len(os.read(self.event_pipe_read, 1024)) >= 0):
                pass
        except OSError, e:
            if e.errno == EAGAIN:
                return
            else:
                raise

    def notify_event(self):
        try:
            os.write(self.event_pipe_write, "X")
        except OSError, e:
            if e.errno == EAGAIN:
                # Event pipe is full -- that's fine, the thread will wake next time it selects.
                return
            else:
                raise

    def post_event(self, ev):
        with self._lock:
            self.event_queue.append(ev)
            self.notify_event()

    def dispatch_events(self):
        with self._lock:
            ret = (len(self.event_queue) > 0)
            to_run = self.event_queue
            self.event_queue = []
            self.drain_event_pipe()
        for event in to_run:
            event()
        return ret

    def get_select_fds(self):
        return [self.event_pipe_read], [], []

    # Called after all event-posting and dispatching is complete
    def cleanup(self):
        os.close(self.event_pipe_read)
        os.close(self.event_pipe_write)

class pycURLThread:

    def __init__(self):
        self.thread = None
        self.curl_ctx = pycurl.CurlMulti()
        self.curl_ctx.setopt(pycurl.M_PIPELINING, 0)
        self.curl_ctx.setopt(pycurl.M_MAXCONNECTS, 20)
        self.active_fetches = []
        self.event_queue = SelectableEventQueue()
        self.event_sources = []
        self.thread = threading.Thread(target=self.pycurl_main_loop, name="Ciel pycURL Thread")
        self.dying = False

    def subscribe(self, bus):
        bus.subscribe('start', self.start, 75)
        bus.subscribe('stop', self.stop, 10)

    def start(self):
        self.thread.start()

    # Called from cURL thread
    def add_fetch(self, new_context):
        self.active_fetches.append(new_context)
        self.curl_ctx.add_handle(new_context.curl_ctx)

    # Called from cURL thread
    def add_event_source(self, src):
        self.event_sources.append(src)

    def do_from_curl_thread(self, callback):
        if threading.current_thread().ident == self.thread.ident:
            callback()
        else:
            self.event_queue.post_event(callback)

    def call_and_signal(self, callback, e, ret):
        ret.ret = callback()
        e.set()

    class ReturnBucket:
        def __init__(self):
            self.ret = None

    def do_from_curl_thread_sync(self, callback):
        if threading.current_thread().ident == self.thread.ident:
            return callback()
        else:
            e = threading.Event()
            ret = pycURLThread.ReturnBucket()
            self.event_queue.post_event(lambda: self.call_and_signal(callback, e, ret))
            e.wait()
            return ret.ret

    def _stop_thread(self):
        self.dying = True
    
    def stop(self):
        self.event_queue.post_event(self._stop_thread)

    def remove_fetch(self, ctx):
        self.curl_ctx.remove_handle(ctx.curl_ctx)
        self.active_fetches.remove(ctx)

    def pycurl_main_loop(self):
        while True:
            # Curl-perform and process events until there's nothing left to do
            while True:
                go_again = False
                # Perform until cURL has nothing left to do
                while True:
                    ret, num_handles = self.curl_ctx.perform()
                    if ret != pycurl.E_CALL_MULTI_PERFORM:
                        break
                # Fire callbacks on completed fetches
                while True:
                    num_q, ok_list, err_list = self.curl_ctx.info_read()
                    if len(ok_list) > 0 or len(err_list) > 0:
                        go_again = True
                    for c in ok_list:
                        self.curl_ctx.remove_handle(c)
                        response_code = c.getinfo(pycurl.RESPONSE_CODE)
#                        ciel.log.error("Curl success: %s -- %s" % (c.ctx.description, str(response_code)))
                        if str(response_code).startswith("2"):
                            c.ctx.success()
                        else:
                            ciel.log.error("Curl failure: HTTP %s" % str(response_code), "CURL_FETCH", logging.WARNING)
                            c.ctx.failure(response_code, "")
                        self.active_fetches.remove(c.ctx)
                    for c, errno, errmsg in err_list:
                        self.curl_ctx.remove_handle(c)
                        ciel.log.error("Curl failure: %s, %s" % 
                                           (str(errno), str(errmsg)), "CURL_FETCH", logging.WARNING)
                        c.ctx.failure(errno, errmsg)
                        self.active_fetches.remove(c.ctx)
                    if num_q == 0:
                        break
                # Process events, both from out-of-thread and due to callbacks
                if self.event_queue.dispatch_events():
                    go_again = True
                if self.dying:
                    return
                if not go_again:
                    break
            if self.dying:
                return
            # Alright, all work appears to be done for now. Gather reasons to wake up.
            # Reason #1: cURL has work to do.
            read_fds, write_fds, exn_fds = self.curl_ctx.fdset()
            # Reason #2: out-of-thread events arrived.
            ev_rfds, ev_wfds, ev_exfds = self.event_queue.get_select_fds()
            # Reason #3: an event source has an interesting event
            for source in self.event_sources:
                rfds, wfds, efds = source.get_select_fds()
                ev_rfds.extend(rfds)
                ev_wfds.extend(wfds)
                ev_exfds.extend(efds)
            read_fds.extend(ev_rfds)
            write_fds.extend(ev_wfds)
            exn_fds.extend(ev_exfds)
            active_read, active_write, active_exn = select.select(read_fds, write_fds, exn_fds)
            for source in self.event_sources:
                source.notify_fds(active_read, active_write, active_exn)

singleton_pycurl_thread = None

def create_pycurl_thread(bus):
    global singleton_pycurl_thread
    singleton_pycurl_thread = pycURLThread()
    singleton_pycurl_thread.subscribe(bus)

def do_from_curl_thread(x):
    singleton_pycurl_thread.do_from_curl_thread(x)

def do_from_curl_thread_sync(x):
    return singleton_pycurl_thread.do_from_curl_thread_sync(x)

def add_fetch(x):
    singleton_pycurl_thread.add_fetch(x)

def remove_fetch(x):
    singleton_pycurl_thread.remove_fetch(x)

def add_event_source(src):
    do_from_curl_thread(lambda: singleton_pycurl_thread.add_event_source(src))
