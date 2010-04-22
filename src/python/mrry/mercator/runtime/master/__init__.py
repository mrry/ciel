'''
Created on 11 Feb 2010

@author: dgm36
'''
from __future__ import with_statement
#from mrry.mercator.master.datamodel import JobManagerPool
from mrry.mercator.runtime.master.master_view import MasterRoot
from mrry.mercator.runtime.master.data_store import GlobalNameDirectory
from mrry.mercator.runtime.master.worker_pool import WorkerPool
from mrry.mercator.runtime.block_store import BlockStore
from mrry.mercator.runtime.task_executor import TaskExecutorPlugin
from mrry.mercator.runtime.master.local_master_proxy import LocalMasterProxy
from mrry.mercator.runtime.master.task_pool import TaskPool
import mrry.mercator
from mrry.mercator.runtime.master.scheduler import Scheduler
import simplejson
import logging
import urllib2
import urllib
import httplib2
import tempfile
import socket
import cherrypy

def master_main(options):

    global_name_directory = GlobalNameDirectory()

    task_pool = TaskPool(cherrypy.engine, global_name_directory)
    task_pool.subscribe()
    
    worker_pool = WorkerPool(cherrypy.engine)
    worker_pool.subscribe()

    local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    master_proxy = LocalMasterProxy(task_pool, None, global_name_directory)
    block_store = BlockStore(local_hostname, local_port, tempfile.mkdtemp(), master_proxy)
    master_proxy.block_store = block_store

    scheduler = Scheduler(cherrypy.engine, task_pool, worker_pool)
    scheduler.subscribe()
    
    root = MasterRoot(task_pool, worker_pool, block_store, global_name_directory)

    cherrypy.tree.mount(root, "", None)
    
    if hasattr(cherrypy.engine, "signal_handler"):
        cherrypy.engine.signal_handler.subscribe()
    if hasattr(cherrypy.engine, "console_control_handler"):
        cherrypy.engine.console_control_handler.subscribe()

    cherrypy.engine.start()
    
    
    
    if options.workerlist is not None:
        master_details = {'netloc': '%s:%d' % (local_hostname, local_port)}
        master_details_as_json = simplejson.dumps(master_details)
        with (open(options.workerlist, "r")) as f:
            for worker_url in f.readlines():
                try:
                    http = httplib2.Http()
                    http.request(urllib2.urlparse.urljoin(worker_url, '/master/'), "POST", master_details_as_json)
                    # Worker will be created by a callback.
                except:
                    cherrypy.log.error("Error adding worker: %s" % (worker_url, ), "WORKER", logging.WARNING)
                    
    cherrypy.engine.block()

#    sch = SchedulerProxy(cherrypy.engine)
#    sch.subscribe()
#
#    reaper = WorkerReaper(cherrypy.engine)
#    reaper.subscribe()
#
#    wr = WorkflowRunner(cherrypy.engine)
#    wr.subscribe()
#
#    te = TaskExecutor(cherrypy.engine)
#    te.subscribe()
#
#    ph = PingHandler(cherrypy.engine)
#    ph.subscribe()

if __name__ == '__main__':
    mrry.mercator.main("master")
