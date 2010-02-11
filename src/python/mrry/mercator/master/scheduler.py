'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy.process import plugins

class Scheduler(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
    
    def subscribe(self):
        self.bus.subscribe('ping', self.ping)
        
    def ping(self, message):
        pass
    
    def create_workflow(self, workflow):
        pass