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

'''
Created on 4 Feb 2010

@author: dgm36
'''

from cherrypy.process import plugins
from Queue import Queue, Empty
from mrry.mercator.runtime.plugins import THREAD_TERMINATOR
import threading
    
class Pinger(plugins.SimplePlugin):
    
    def __init__(self, bus, master_proxy, status_provider=None, ping_timeout=30):
        plugins.SimplePlugin.__init__(self, bus)
        self.queue = Queue()
        self.non_urgent_queue = Queue()
        self.is_running = False
        self.thread = None
        self.master_proxy = master_proxy
        self.status_provider = status_provider
        self.ping_timeout = ping_timeout
                
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('ping_now', self.ping_now)
        self.bus.subscribe('ping_non_urgent', self.ping_non_urgent)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        self.bus.unsubscribe('ping_now', self.ping_now)
        self.bus.unsubscribe('ping_non_urgent', self.ping_non_urgent)
                
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
    
    def ping_now(self, message=None):
        self.queue.put(message)
        
    def ping_non_urgent(self, message):
        self.non_urgent_queue.put(message)
        
    def thread_main(self):
        while True:
            
            ping_news = []
            
            try:
                new_thing = self.queue.get(block=True, timeout=self.ping_timeout)
                if not self.is_running or new_thing is THREAD_TERMINATOR:
                    pass
                elif new_thing is not None:
                    ping_news.append(new_thing)
            except Empty:
                pass
            
            if self.is_running:
                
                try:
                    while True:
                        ping_news.append(self.queue.get_nowait())
                except Empty:
                    pass
                
                try:
                    while True:
                        ping_news.append(self.non_urgent_queue.get_nowait())
                except Empty:
                    pass
            
            if self.status_provider is not None:
                status = self.status_provider.current_status()
            else:
                status = None 
            
            try:
                self.master_proxy.ping(status, ping_news)
            except:
                for news in ping_news:
                    self.non_urgent_queue.put(news)
            
            if not self.is_running:
                break
