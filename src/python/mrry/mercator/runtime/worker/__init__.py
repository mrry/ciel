'''
Created on 4 Feb 2010

@author: dgm36
'''
from mrry.mercator.runtime.worker.master_proxy import MasterProxy
from mrry.mercator.runtime.task_executor import TaskExecutorPlugin
from mrry.mercator.runtime.block_store import BlockStore
from mrry.mercator.runtime.worker.worker_view import WorkerRoot,\
    build_worker_descriptor
from mrry.mercator.runtime.executors import ExecutionFeatures
import tempfile
import cherrypy
import mrry.mercator
import httplib2
import os
import socket
import urlparse
import simplejson

def register_with_master(master_uri, execution_features):
    http = httplib2.Http()

    master_register_url = urlparse.urljoin(master_uri, 'worker/')
    print master_register_url
    
    (response, content) = http.request(uri=master_register_url, method='POST', body=simplejson.dumps(build_worker_descriptor(execution_features)))

    print response

    if response.status != 200:
        print response
        print content
        raise cherrypy.HTTPError(response.status)

def worker_main(options):
    
    local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    
    if options.master is not None:
        master_netloc = urlparse.urlparse(options.master).netloc
    else:
        master_netloc = None
        
    master_proxy = MasterProxy(master_netloc)
    
    block_store = BlockStore(local_hostname, local_port, tempfile.mkdtemp(), master_proxy)
    
    execution_features = ExecutionFeatures()
    
    task_executor = TaskExecutorPlugin(cherrypy.engine, block_store, master_proxy, execution_features, 1)
    task_executor.subscribe()
    
    
    root = WorkerRoot(master_proxy, block_store, execution_features)
    
    cherrypy.tree.mount(root, "", None)
    
    if hasattr(cherrypy.engine, "signal_handler"):
        cherrypy.engine.signal_handler.subscribe()
    if hasattr(cherrypy.engine, "console_control_handler"):
        cherrypy.engine.console_control_handler.subscribe()

    cherrypy.engine.start()
    
    try:
        if options.master is not None:
            register_with_master(options.master, execution_features)
    except:
        pass
    
    cherrypy.engine.block()
        
#    pinger = Pinger(cherrypy.engine, options.master, worker_id) 
#    pinger.subscribe()
    
if __name__ == '__main__':
    mrry.mercator.main("worker")