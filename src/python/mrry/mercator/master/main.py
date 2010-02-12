'''
Created on 11 Feb 2010

@author: dgm36
'''
from mrry.mercator.master.server_root import MasterRoot
from mrry.mercator.master.datamodel import JobManagerPool
import cherrypy

if __name__ == '__main__':
    
    pool = Scheduler(cherrypy.engine)
    pool.subscribe()
    
    
    
    root = MasterRoot(scheduler)
    
    cherrypy.quickstart(root)