import struct
import sys
import threading

class MercatorIPC:
    
    def __init__(self, callback=None, in_channel=sys.stdin, out_channel=sys.stdout):
        self.in_channel = in_channel
        self.out_channel = out_channel
        self.callback = callback
        self.out_lock = threading.Lock()
        self.terminating = False
        self.thread = threading.Thread(target=self.thread_main, args=())
        self.thread.start()
    
    def send_message(self, message):
        with self.out_lock:
            self.out_channel.write(struct.pack("I", len(message)))
            self.out_channel.write(message)
            self.out_channel.flush()
            
    def finalize(self):
        self.terminating = True
        self.send_message("")
        self.thread.join()
    
    def thread_main(self):
        while True:
            msg_len = struct.unpack("I", self.in_channel.read(struct.calcsize("I")))
            if msg_len == 0:
                if self.terminating:
                    return
                elif self.callback is not None:
                    try:
                        self.callback("")
                    except:
                        pass
            else:
                message = self.in_channel.read(msg_len)
                if not self.terminating and self.callback is not None:
                    try:
                        self.callback(message)
                    except:
                        pass
    