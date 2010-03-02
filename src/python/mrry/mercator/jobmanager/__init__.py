'''
Created on 4 Feb 2010

@author: dgm36
'''
import cherrypy
import mrry.mercator
from mrry.mercator.jobmanager.plugins import JobExecutor, StatusMaintainer,\
    Pinger, DataManager
from mrry.mercator.jobmanager.server_root import JobsRoot
import httplib2
import os
import socket
import urlparse
import simplejson

def register_with_master(options):
    http = httplib2.Http()
    
    worker_name = "http://%s:%d/" % (socket.getfqdn(), cherrypy.config.get('server.socket_port'))
    
    master_register_url = urlparse.urljoin(options.master, 'worker/')
    
    (response, content) = http.request(uri=master_register_url, method='POST', body=simplejson.dumps({'uri' : worker_name}))

    if response.status != 200:
        print response
        print content
        raise
    
    return simplejson.loads(content)

def jobmanager_main(options):
    
    worker_id = register_with_master(options)
    cherrypy.config.update({'worker.id' : worker_id})
    
    jr = JobExecutor(cherrypy.engine, 10)
    jr.subscribe()
    sm = StatusMaintainer(cherrypy.engine)
    sm.subscribe()
    dm = DataManager(cherrypy.engine)
    dm.subscribe()
    root = JobsRoot(sm, jr, dm)
    
    pinger = Pinger(cherrypy.engine, options.master, worker_id) 
    pinger.subscribe()
    
    cherrypy.quickstart(root)
    
if __name__ == '__main__':
    mrry.mercator.main("jobmanager")