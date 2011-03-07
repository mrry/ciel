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
import ciel

'''
Created on 15 Apr 2010

@author: dgm36
'''
from urlparse import urljoin
from skywriting.runtime.block_store import SWReferenceJSONEncoder,\
    json_decode_object_hook
from skywriting.runtime.exceptions import MasterNotRespondingException,\
    WorkerShutdownException, RuntimeSkywritingError
import logging
import random
import cherrypy
import socket
import httplib2
from threading import Event

import simplejson

def get_worker_netloc():
    return '%s:%d' % (socket.getfqdn(), cherrypy.config.get('server.socket_port'))

class MasterProxy:
    
    def __init__(self, worker, bus, master_url=None):
        self.bus = bus
        self.worker = worker
        self.master_url = master_url
        self.stop_event = Event()

    def subscribe(self):
        # Stopping is high-priority
        self.bus.subscribe("stop", self.handle_shutdown, 10)


    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.handle_shutdown)

    def change_master(self, master_url):
        self.master_url = master_url
        
    def get_master_details(self):
        return {'netloc': self.master_netloc}

    def handle_shutdown(self):
        self.stop_event.set()
    
    def backoff_request(self, url, method, payload=None, num_attempts=1, initial_wait=0):
        initial_wait = 5
        for _ in range(0, num_attempts):
            if self.stop_event.is_set():
                break
            try:
                # This sucks: httplib2 doesn't have any sort of cancellation method, so if the worker
                # is shutting down we must wait for this request to fail or time out.
                response, content = httplib2.Http().request(url, method, payload)
                if response.status == 200:
                    return response, content
                else:
                    ciel.log.error("Error contacting master", "MSTRPRXY", logging.WARN, False)
                    ciel.log.error("Response was: %s" % str(response), "MSTRPRXY", logging.WARN, False)
                    raise MasterNotRespondingException()
            except:
                ciel.log.error("Error contacting master", "MSTRPRXY", logging.WARN, True)
            self.stop_event.wait(initial_wait)
            initial_wait += initial_wait * random.uniform(0.5, 1.5)
        ciel.log.error("Given up trying to contact master", "MSTRPRXY", logging.ERROR, True)
        if self.stop_event.is_set():
            raise WorkerShutdownException()
        else:
            raise MasterNotRespondingException()

    def get_public_hostname(self):
        message_url = urljoin(self.master_url, "control/gethostname/")
        _, result = self.backoff_request(message_url, 'GET')
        return simplejson.loads(result)

    def register_as_worker(self):
        message_payload = simplejson.dumps(self.worker.as_descriptor())
        message_url = urljoin(self.master_url, 'control/worker/')
        _, result = self.backoff_request(message_url, 'POST', message_payload)
        self.worker.id = simplejson.loads(result)
    
    def publish_refs(self, job_id, task_id, refs):
        message_payload = simplejson.dumps(refs, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/publish' % (job_id, task_id))
        self.backoff_request(message_url, "POST", message_payload)

    def report_tasks(self, job_id, root_task_id, report):
        message_payload = simplejson.dumps(report, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/report' % (job_id, root_task_id))
        self.backoff_request(message_url, "POST", message_payload)

    def failed_task(self, job_id, task_id, reason=None, details=None, bindings={}):
        message_payload = simplejson.dumps((reason, details, bindings), cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/failed' % (job_id, task_id))
        self.backoff_request(message_url, "POST", message_payload)

    def ping(self):
        message_url = urljoin(self.master_url, 'control/worker/%s/ping/' % (str(self.worker.id), ))
        self.backoff_request(message_url, "POST", "PING", 1, 0)
