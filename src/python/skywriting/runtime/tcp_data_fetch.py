
from shared.references import SW2_SocketStreamReference
from skywriting.runtime.remote_stat import subscribe_remote_output_nopost, unsubscribe_remote_output_nopost
from skywriting.runtime.block_store import get_own_netloc
import threading
import ciel
import logging
import socket
import os

class TcpTransferContext:
    
    def __init__(self, ref, chunk_size, fetch_ctx):
        self.ref = ref
        assert isinstance(ref, SW2_SocketStreamReference)
        self.otherend_hostname = self.ref.socket_netloc.split(":")[0]
        self.chunk_size = chunk_size
        self.fetch_ctx = fetch_ctx
        self.thread = threading.Thread(target=self.thread_main)
        self.lock = threading.Lock()
        self.done = False
        self.should_close = False

    def start(self):
        ciel.log("Stream-fetch %s: trying TCP (%s:%s)" % (self.ref.id, self.otherend_hostname, self.ref.socket_port), "TCP_FETCH", logging.INFO)
        self.thread.start()

    def thread_main(self):
        try:
            with self.lock:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.should_close = True            
            ciel.log("Connecting %s:%s" % (self.otherend_hostname, self.ref.socket_port), "TCP_FETCH", logging.INFO)
            subscribe_remote_output_nopost(self.ref.id, self)
            self.sock.connect((self.otherend_hostname, self.ref.socket_port))
            self.sock.sendall("%s %s %d\n" % (self.ref.id, get_own_netloc(), self.chunk_size))
            ciel.log("%s:%s connected: requesting %s (chunk size %d)" % (self.otherend_hostname, self.ref.socket_port, self.ref.id, self.chunk_size), "TCP_FETCH", logging.INFO)
            fp = self.sock.makefile("r", bufsize=0)
            response = fp.readline().strip()
            fp.close()
            with self.lock:
                self.should_close = False
                if response.find("GO") != -1:
                    ciel.log("TCP-fetch %s: transfer started" % self.ref.id, "TCP_FETCH", logging.INFO)
                    new_fd = os.dup(self.sock.fileno())
                    self.sock.close()
                    self.fetch_ctx.set_fd(new_fd, True)
                else:
                    ciel.log("TCP-fetch %s: request failed: other end said '%s'" % (self.ref.id, response), "TCP_FETCH", logging.WARNING)
                    unsubscribe_remote_output_nopost(self.ref.id)
                    self.done = True
                    self.sock.close()
                    self.fetch_ctx.result(False)
        except Exception as e:
            unsubscribe_remote_output_nopost(self.ref.id)
            ciel.log("TCP-fetch %s: failed due to exception %s" % (self.ref.id, repr(e)), "TCP_FETCH", logging.ERROR)
            with self.lock:
                if self.should_close:
                    self.sock.close()
                self.done = True
                self.should_close = False
            self.fetch_ctx.result(False)

    def unsubscribe(self, fetcher):
        should_callback = False
        with self.lock:
            if self.done:
                return
            else:
                if self.should_close:
                    self.sock.close()
                self.done = True
                should_callback = True
                unsubscribe_remote_output_nopost(self.ref.id)
        if should_callback:
            self.fetch_ctx.result(False)

    def advertisment(self, bytes=None, done=None, absent=None, failed=None):
        if not self.done:
            self.done = True
            if failed is True:
                ciel.log("TCP-fetch %s: remote reported failure" % self.ref.id, "TCP_FETCH", logging.ERROR)
                self.fetch_ctx.result(False)
            elif done is True:
                ciel.log("TCP-fetch %s: remote reported success (%d bytes)" % (self.ref.id, bytes), "TCP_FETCH", logging.INFO)
                self.fetch_ctx.result(True)
            else:
                ciel.log("TCP-fetch %s: weird advertisment (%s, %s, %s, %s)" % (bytes, done, absent, failed), "TCP_FETCH", logging.ERROR)
                self.fetch_ctx.result(False)
        else:
            ciel.log("TCP-fetch %s: ignored advertisment as transfer is done" % self.ref.id, "TCP_FETCH", logging.WARNING)
