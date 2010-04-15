'''
Created on Apr 15, 2010

@author: derek
'''
from cherrypy.process import plugins

class WorkerPool(plugins.SimplePlugin):
    
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)