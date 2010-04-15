'''
Created on 11 Feb 2010

@author: dgm36
'''
#from mrry.mercator.master.datamodel import JobManagerPool
from mrry.mercator.runtime.master.master_view import MasterRoot
from mrry.mercator.runtime.master.data_store import GlobalNameDirectory
from mrry.mercator.runtime.master.worker_pool import WorkerPool
from mrry.mercator.runtime.worker.data_store import BlockStore
from mrry.mercator.runtime.task_executor import TaskExecutorPlugin
from mrry.mercator.runtime.master.local_master_proxy import LocalMasterProxy
from mrry.mercator.runtime.master.task_pool import TaskPool
import mrry.mercator
import cherrypy

def master_main(options):


    block_store = BlockStore()
    global_name_directory = GlobalNameDirectory()
    
    task_pool = TaskPool(cherrypy.engine, global_name_directory)
    task_pool.subscribe()
    
    worker_pool = WorkerPool(cherrypy.engine)
    worker_pool.subscribe()

    master_proxy = LocalMasterProxy(task_pool, block_store, global_name_directory)

    task_executor = TaskExecutorPlugin(cherrypy.engine, block_store, master_proxy, 1)
    task_executor.subscribe()
    
    root = MasterRoot(worker_pool, block_store, global_name_directory)
    cherrypy.quickstart(root)

    if options.workerlist is not None:
        with (open(options.workerlist, "r")) as f:
            for worker_url in f.readlines():
                worker_pool.add_worker(worker_url)

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