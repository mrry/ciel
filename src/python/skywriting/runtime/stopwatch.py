'''
Created on Jun 21, 2011

@author: derek
'''
import datetime

class Stopwatch:
    def __init__(self):
        self.enabled = False 
        self.times = {}
        self.starts = {}
        
    def enable(self):
        self.enabled = True
        
    def start(self, name):
        if self.enabled:
            self.starts[name] = datetime.datetime.now()
        
    def stop(self, name):
        try:
            start = self.starts.pop(name)
            finish = datetime.datetime.now()
            
            try:
                time_list = self.times[name]
            except KeyError:
                time_list = []
                self.times[name] = time_list
            
            time_list.append(finish - start)
            
        except KeyError:
            pass
        
    def get_times(self, name):
        return self.times[name]