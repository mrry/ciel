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
        
    def _start_at(self, name, at):
        self.starts[name] = at
        
    def start(self, name):
        if self.enabled:
            self._start_at(name, datetime.datetime.now())
        
    def _stop_at(self, name, at):
        try:
            start = self.starts.pop(name)
            finish = at
            
            try:
                time_list = self.times[name]
            except KeyError:
                time_list = []
                self.times[name] = time_list
            
            time_list.append(finish - start)
            
        except KeyError:
            pass
        
    def stop(self, name):
        if self.enabled:
            self._stop_at(name, datetime.datetime.now())
        
    def lap(self, name):
        if self.enabled:
            lap_time = datetime.datetime.now()
            self._stop_at(name, lap_time)
            self._start_at(name, lap_time)
    
    def multi(self, starts=[], stops=[], laps=[]):
        if self.enabled:
            now = datetime.datetime.now()
            for start_name in starts:
                self._start_at(start_name, now)
            for stop_name in stops:
                self._stop_at(stop_name, now)
            for lap_name in laps:
                self._stop_at(lap_name, now)
                self._start_at(lap_name, now)
                
    def get_times(self, name):
        return self.times[name]