'''
Created on 11 Feb 2010

@author: dgm36
'''
from mrry.mercator.master.server_root import MasterRoot
import mrry.mercator
from mrry.mercator.master.scheduler import TaskExecutor
#from mrry.mercator.master.datamodel import JobManagerPool
import cherrypy

def pinger(update):
    print "PING RECEIVED: %s" % (str(update), )

def master_main(options):
#    pool = Scheduler(cherrypy.engine)
#    pool.subscribe()

    

    te = TaskExecutor(cherrypy.engine)
    te.subscribe()

    root = MasterRoot()
    
    cherrypy.engine.subscribe('ping', pinger)
    
    cherrypy.quickstart(root)

if __name__ == '__main__':
    mrry.mercator.main("master")