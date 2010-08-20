'''
Created on 17 Aug 2010

@author: dgm36
'''
from skywriting.runtime.plugins import AsynchronousExecutePlugin
from threading import Timer

class DeferredWorkPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, event_name="deferred_work"):
        AsynchronousExecutePlugin.__init__(self, bus, 1, event_name)
        self.timers = {}
        self.current_timer_id = 0
    
    def stop(self):
        for timer in self.timers.values():
            timer.cancel()
        AsynchronousExecutePlugin.stop(self)
    
    def handle_input(self, input):
        input()
        
    def do_deferred(self, callable):
        self.receive_input(callable)
        
    def _handle_deferred_after(self, callable, timer_id):
        del self.timers[timer_id]
        callable()
        
    def do_deferred_after(self, secs, callable):
        timer_id = self.current_timer_id
        self.current_timer_id += 1
        t = Timer(secs, self.do_deferred, args=(lambda: self._handle_deferred_after(callable, timer_id), ))
        self.timers[timer_id] = t
        t.start()
        