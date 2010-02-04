'''
Created on 1 Feb 2010

@author: dgm36
'''
from threading import Condition, Lock

pending_lock = Lock()
pending = {}

class PendingRequest:
    
    def __init__(self):
        self.response = None
        self.cv = Condition()
        
    def update(self, response):
        self.cv.acquire()
        self.response = response
        self.cv.notifyAll()
        self.cv.release()
        
    def block(self, timeout):
        self.cv.acquire()
        if self.response is None:
            self.cv.wait(timeout)
        self.cv.release()
        return self.response

def start_response_handler(response_queue):
    while True:
        response = response_queue.get()
        if response.type == 'QUERY_RESPONSE':
            with pending_lock:
                pending[response.requestID].update(response)
        else:
            print "Skipping response: %s" % (str(response), )

def do_synchronous_request(request_msg, job, timeout=None):
    pendingRequest = PendingRequest()
    pending[request_msg.id] = pendingRequest
    job.requestQueue.put(request_msg)
    response = pendingRequest.block(timeout)
    del pending[request_msg.id]
    return response