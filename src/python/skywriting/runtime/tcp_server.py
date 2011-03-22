
import skywriting.runtime.pycurl_thread as pct

import threading
import socket
import os
import ciel

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
                sock_pusher = SocketPusher(output_id, new_sock, int(chunk_size))
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

class TcpServer:

    def __init__(self, port):
        self.aux_port = port
        ciel.log("Listening for auxiliary connections on port %d" % port, "TCP_FETCH", logging.INFO)
        self.aux_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.aux_listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.aux_listen_socket.bind(("0.0.0.0", port))
        self.aux_listen_socket.listen(5)
        self.aux_listen_socket.setblocking(False)

    def get_select_fds(self):
        return [self.aux_listen_socket.fileno()], [], []

    def notify_fds(self, read_fds, _, _):
        if self.aux_listen_socket.fileno() in read_fds:
            (new_sock, _) = self.aux_listen_socket.accept()
            new_aux_connection(new_sock)

aux_listen_port = None

def create_tcp_server(port):
    global aux_listen_port
    aux_listen_port = port
    pct.add_event_source(TcpServer(port))

def tcp_server_active():
    return aux_listen_port is not None
