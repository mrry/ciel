# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
import ciel
import logging

'''
Created on 4 Feb 2010

@author: dgm36
'''

from cherrypy.process import plugins
from Queue import Queue, Empty
from skywriting.runtime.plugins import THREAD_TERMINATOR
import threading
    
class PingerPoker:
    pass
PINGER_POKER = PingerPoker()

class PingFailed:
    pass
PING_FAILED = PingFailed()
    
class Pinger(plugins.SimplePlugin):
    '''
    The pinger maintains the connection between a worker and the master. It
    serves two roles:
    
    1. At start up, the pinger is responsible for making the initial connection
       to the master. It periodically attempts registration.
       
    2. During normal operation, the pinger sends a heartbeat message every 30
       seconds to the master. The timeout is configurable, using the
       ping_timeout argument to the constructor.
       
    If the ping should fail, the pinger will attempt to re-register with a 
    master at the same URL. The master can be changed using the /master/ URI on
    the worker.
    '''
    
    def __init__(self, bus, master_proxy, status_provider=None, ping_timeout=5):
        plugins.SimplePlugin.__init__(self, bus)
        self.queue = Queue()
        self.non_urgent_queue = Queue()
        
        self.is_connected = False
        self.is_running = False
        
        self.thread = None
        self.master_proxy = master_proxy
        self.status_provider = status_provider
        self.ping_timeout = ping_timeout
                
    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('poke', self.poke)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        self.bus.unsubscribe('poke', self.poke)
               
    def ping_fail_callback(self, success, url):
        if not success:
            ciel.log("Sending ping to master failed", "PINGER", logging.WARNING)
            self.queue.put(PING_FAILED)
                
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self.thread_main, args=())
            self.thread.start()
    
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.queue.put(THREAD_TERMINATOR)
            self.thread.join()
            self.thread = None
        
    def poke(self):
        self.queue.put(PINGER_POKER)
        
    def thread_main(self):
        
        while True:
            
            # While not connected, attempt to register as a new worker.
            while not self.is_connected:
            
                try:    
                    hostname = self.master_proxy.get_public_hostname()
                    self.master_proxy.worker.set_hostname(hostname)
                    self.master_proxy.register_as_worker()
                    self.is_connected = True
                    break
                except:
                    pass
            
                try:
                    maybe_terminator = self.queue.get(block=True, timeout=self.ping_timeout)
                    if not self.is_running or maybe_terminator is THREAD_TERMINATOR:
                        return
                except Empty:
                    pass
            
            
            # While connected, periodically ping the master.
            while self.is_connected:
                
                try:
                    new_thing = self.queue.get(block=True, timeout=self.ping_timeout)
                    if new_thing is PING_FAILED:
                        self.is_connected = False
                        break
                    if not self.is_connected or not self.is_running or new_thing is THREAD_TERMINATOR:
                        break
                except Empty:
                    pass
                
                try:
                    self.master_proxy.ping(self.ping_fail_callback)
                except:
                    self.is_connected = False
                
            if not self.is_running:
                break