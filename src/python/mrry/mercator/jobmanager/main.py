'''
Created on 4 Feb 2010

@author: dgm36
'''
import cherrypy
from mrry.mercator.jobmanager.plugins import JobRunner, StatusMaintainer
from mrry.mercator.jobmanager.server_root import JobsRoot

if __name__ == '__main__':
    jr = JobRunner(cherrypy.engine, 10)
    jr.subscribe()
    sm = StatusMaintainer(cherrypy.engine)
    sm.subscribe()
    
    root = JobsRoot(sm, jr)

    cherrypy.quickstart(root)