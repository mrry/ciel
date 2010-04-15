'''
Created on 4 Feb 2010

@author: dgm36
'''
from mrry.mercator.runtime.worker.master_proxy import MasterProxy
from mrry.mercator.runtime.task_executor import TaskExecutorPlugin
from mrry.mercator.runtime.block_store import BlockStore
from mrry.mercator.runtime.worker.worker_view import WorkerRoot
from mrry.mercator.runtime.worker.features import WorkerFeatures
import tempfile
import cherrypy
import mrry.mercator
import httplib2
import os
import socket
import urlparse
import simplejson

def register_with_master(master_uri, local_hostname, local_port):
    http = httplib2.Http()
    
    local_netloc = "%s:%d" % (local_hostname, local_port)
    
    master_register_url = urlparse.urljoin(master_uri, 'worker/')
    
    (response, content) = http.request(uri=master_register_url, method='POST', body=simplejson.dumps({'netloc': local_netloc, 'features': {}}))

    if response.status != 200:
        print response
        print content
        raise cherrypy.HTTPError(response.status)
    

def worker_main(options):
    
    local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    
    master_proxy = MasterProxy()
    block_store = BlockStore(local_hostname, local_port, tempfile.mkdtemp(), master_proxy)
    
    task_executor = TaskExecutorPlugin(cherrypy.engine, block_store, master_proxy, 1)
    task_executor.subscribe()
    
    worker_id = register_with_master(options)
    cherrypy.config.update({'worker.id' : worker_id})
    
    node_features = WorkerFeatures()
    
    root = WorkerRoot(master_proxy, block_store, node_features)
    
    cherrypy.quickstart(root)
    
    if options.master is not None:
        register_with_master(options.master, local_hostname, local_port)
        
#    pinger = Pinger(cherrypy.engine, options.master, worker_id) 
#    pinger.subscribe()
    
if __name__ == '__main__':
    mrry.mercator.main("worker")