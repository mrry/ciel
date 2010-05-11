# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

'''
Created on 15 Apr 2010

@author: dgm36
'''
from urlparse import urljoin
from mrry.mercator.runtime.references import SWDataValue
from mrry.mercator.runtime.block_store import SWReferenceJSONEncoder,\
    json_decode_object_hook
from mrry.mercator.runtime.exceptions import MasterNotRespondingException
import logging
import time
import random
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
    
    def backoff_request(self, url, method, payload=None, num_attempts=3, initial_wait=5):
        initial_wait = 5
        for i in range(0, num_attempts):
            try:
                response, content = httplib2.Http().request(url, method, payload)
                if response.status == 200:
                    return response, content
                else:
                    cherrypy.log.error("Error contacting master", "MSTRPRXY", logging.WARN, True)
                    cherrypy.log.error("Response was: %s" % str(response), "MSTRPRXY", logging.WARN, True)
            except:
                cherrypy.log.error("Error contacting master", "MSTRPRXY", logging.WARN, True)
            time.sleep(initial_wait)
            initial_wait += initial_wait * random.uniform(0.5, 1.5)
        cherrypy.log.error("Given up trying to contact master", "MSTRPRXY", logging.ERROR, True)
        raise MasterNotRespondingException()
    
    def register_as_worker(self):
        message_payload = simplejson.dumps(self.worker.as_descriptor())
        message_url = urljoin(self.master_url, 'worker/')
        _, result = self.backoff_request(message_url, 'POST', message_payload)
        self.worker.id = simplejson.loads(result)
    
    def publish_global_refs(self, global_id, refs):
        message_payload = simplejson.dumps(refs, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'global_data/%d' % (global_id, ))
        self.backoff_request(message_url, "POST", message_payload)
        
    def spawn_tasks(self, parent_task_id, tasks):
        message_payload = simplejson.dumps(tasks, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'task/%d/spawn' % (parent_task_id, ))
        (_, result) = self.backoff_request(message_url, "POST", message_payload)
        return simplejson.loads(result)
    
    def commit_task(self, task_id, bindings):
        message_payload = simplejson.dumps(bindings, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'task/%d/commit' % (task_id, ))
        self.backoff_request(message_url, "POST", message_payload)
        
    def failed_task(self, task_id):
        message_payload = simplejson.dumps(task_id)
        message_url = urljoin(self.master_url, 'task/%d/failed' % (task_id, ))
        self.backoff_request(message_url, "POST", message_payload)

    def ping(self, status, ping_news):
        message_payload = simplejson.dumps({'worker': self.worker.netloc(), 'status': status, 'news': ping_news})
        message_url = urljoin(self.master_url, 'worker/%d/ping/' % (self.worker.id, ))
        try:
            self.backoff_request(message_url, "POST", message_payload, 1, 0)
        except MasterNotRespondingException:
            pass
        
    def get_task_descriptor_for_future(self, ref):
        message_url = urljoin(self.master_url, 'global_data/%d/task' % (ref.id, ))
        (_, result) = self.backoff_request(message_url, "GET")
        return simplejson.loads(result, object_hook=json_decode_object_hook)
    
    def abort_production_of_output(self, ref):
        message_url = urljoin(self.master_url, 'global_data/%d' % (ref.id, ))
        self.backoff_request(message_url, "DELETE")