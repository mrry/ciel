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
from __future__ import with_statement
import ciel
from Queue import Queue
import logging
import threading

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class AsynchronousExecutePlugin:
    
    def __init__(self, bus, num_threads, subscribe_event=None, publish_success_event=None, publish_fail_event=None):
        self.bus = bus
        self.threads = []
        
        self.queue = Queue()
        
        self.num_threads = num_threads
        self.subscribe_event = subscribe_event
        self.publish_success_event = publish_success_event
        self.publish_fail_event = publish_fail_event
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        if self.subscribe_event is not None:
            self.bus.subscribe(self.subscribe_event, self.receive_input)
            
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        if self.subscribe_event is not None:
            self.bus.unsubscribe(self.subscribe_event, self.receive_input)
            
    def start(self):
        self.is_running = True
        for _ in range(self.num_threads):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
                
    def stop(self):
        self.is_running = False
        for _ in range(self.num_threads):
            self.queue.put(THREAD_TERMINATOR)
        for thread in self.threads:
            thread.join()
        self.threads = []
        
    def receive_input(self, input=None):
        self.queue.put(input)
        
    def thread_main(self):
        
        while True:
            if not self.is_running:
                break
            input = self.queue.get()
            if input is THREAD_TERMINATOR:
                break

            try:
                result = self.handle_input(input)
                if self.publish_success_event is not None:
                    self.bus.publish(self.publish_success_event, input, result)
            except Exception, ex:
                if self.publish_fail_event is not None:
                    self.bus.publish(self.publish_fail_event, input, ex)
                else:
                    ciel.log.error('Error handling input in %s' % (self.__class__, ), 'PLUGIN', logging.ERROR, True)

    def handle_input(self, input):
        """Override this method to specify the behaviour on processing a single input."""
        pass
