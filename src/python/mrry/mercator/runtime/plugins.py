'''
Created on 13 Apr 2010

@author: dgm36
'''
from cherrypy.process.plugins import SimplePlugin
from Queue import Queue
import threading

class ThreadTerminator:
    pass
THREAD_TERMINATOR = ThreadTerminator()

class AsynchronousExecutePlugin(SimplePlugin):
    
    def __init__(self, bus, num_threads, subscribe_event, publish_success_event=None, publish_fail_event=None):
        SimplePlugin.__init__(self, bus)
        self.threads = []
        
        self.queue = Queue()
        
        self.num_threads = num_threads
        self.subscribe_event = subscribe_event
        self.publish_success_event = publish_success_event
        self.publish_fail_event = publish_fail_event
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe(self.subscribe_event, self.receive_input)
            
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        self.bus.unsubscribe(self.subscribe_event, self.receive_input)
            
    def start(self):
        self.is_running = True
        for i in range(self.num_threads):
            t = threading.Thread(target=self.thread_main, args=())
            self.threads.append(t)
            t.start()
                
    def stop(self):
        self.is_running = False
        for i in range(self.num_threads):
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
            except Exception as ex:
                if self.publish_fail_event is not None:
                    self.bus.publish(self.publish_fail_event, input, ex)
                    
    def handle_input(self, input):
        """Override this method to specify the behaviour on processing a single input."""
        pass