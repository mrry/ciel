'''
Created on 1 Feb 2010

@author: dgm36
'''
from uuid import uuid4
from mrry.mercator.worker.messages import JobCompletedMessage, SwitchRootMessage,\
    QueryResponse
from Queue import Empty
from mrry.mercator.worker.roots import DefaultJobRoot

class Job:

    def __init__(self, requestQueue, responseQueue, pingerQueue):
        self.name = uuid4()
        self.requestQueue = requestQueue
        self.responseQueue = responseQueue
        self.pingerQueue = pingerQueue
    
    def handleRequests(self):
        while True:
            request = self.requestQueue.get()
            
            # Job terminated naturally.
            if request.type == "JOB_TERMINATED":
                # Inform pinger.
                self.pingerQueue.put(JobCompletedMessage(self.name))
                # Inform server to switch to static root.
                self.responseQueue.put(SwitchRootMessage(self.name, self.completedRoot()))
                                       
                break
            else:
                response = self.handleSingleRequest(request)
                self.responseQueue.put(QueryResponse(request.id, response))
                
        while True:
            try:
                request = self.requestQueue.get(timeout=0)
                self.responseQueue.put(QueryResponse(request.id, "TERMINATED"))
                
            except Empty:
                break
    
    def handleSingleRequest(self, request):
        return None
    
    def root(self):
        return DefaultJobRoot(self.name)
    
    def completedRoot(self):
        return DefaultJobRoot(self.name)