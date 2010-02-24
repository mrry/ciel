'''
Created on 4 Feb 2010

@author: dgm36
'''
import cherrypy
import mrry.mercator
from mrry.mercator.jobmanager.plugins import JobExecutor, StatusMaintainer,\
    Pinger
from mrry.mercator.jobmanager.server_root import JobsRoot

def jobmanager_main(options):
    jr = JobExecutor(cherrypy.engine, 10)
    jr.subscribe()
    sm = StatusMaintainer(cherrypy.engine)
    sm.subscribe()
    root = JobsRoot(sm, jr)
    
    pinger = Pinger(cherrypy.engine, options.master, "HELO") 
    pinger.subscribe()
    
    cherrypy.quickstart(root)
    
if __name__ == '__main__':
    mrry.mercator.main("jobmanager")