'''
Created on 15 Apr 2010

@author: dgm36
'''
from urlparse import urljoin
import cherrypy
import socket
import httplib2

import simplejson

def get_worker_netloc():
    return '%s:%d' % (socket.getfqdn(), cherrypy.config.get('server.socket_port'))

class MasterProxy:
    
    def __init__(self, worker, master_url=None):
        self.worker = worker
        self.master_url = master_url
    
    def change_master(self, master_url):
        self.master_url = master_url
        
    def get_master_details(self):
        return {'netloc': self.master_netloc}
    
    def register_as_worker(self):
        message_payload = simplejson.dumps(self.worker.as_descriptor())
        message_url = urljoin(self.master_url, 'worker/')
        (_, result) = httplib2.Http().request(message_url, 'POST', message_payload)
        self.worker.id = simplejson.loads(result)
    
    def publish_global_object(self, global_id, urls):
        message_payload = simplejson.dumps({global_id: urls})
        message_url = urljoin(self.master_url, 'global_data/%d' % (global_id, ))
        httplib2.Http().request(message_url, "POST", message_payload)
        
    def spawn_tasks(self, parent_task_id, tasks):
        print "Spawning %d tasks" % (len(tasks), )
        message_payload = simplejson.dumps(tasks)
        message_url = urljoin(self.master_url, 'task/%d/spawn' % (parent_task_id, ))
        (_, result) = httplib2.Http().request(message_url, "POST", message_payload)
        return simplejson.loads(result)
    
    def commit_task(self, task_id, bindings):
        message_payload = simplejson.dumps(bindings)
        message_url = urljoin(self.master_url, 'task/%d/commit' % (task_id, ))
        httplib2.Http().request(message_url, "POST", message_payload)
        
    def failed_task(self, task_id):
        message_payload = simplejson.dumps(task_id)
        message_url = urljoin(self.master_url, 'task/%d/failed' % (task_id, ))
        httplib2.Http().request(message_url, "POST", message_payload)

    def ping(self, status, ping_news):
        message_payload = simplejson.dumps({'worker': self.worker.netloc(), 'status': status, 'news': ping_news})
        message_url = urljoin(self.master_url, 'worker/%d/ping/' % (self.worker.id, ))
        httplib2.Http().request(message_url, "POST", message_payload)
        
    def get_task_descriptor_for_future(self, ref):
        message_url = urljoin(self.master_url, 'global_data/%d/task' % (ref.id, ))
        (_, result) = httplib2.Http().request(message_url, "GET")
        return simplejson.loads(result)