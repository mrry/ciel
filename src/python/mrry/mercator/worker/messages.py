'''
Created on 1 Feb 2010

@author: dgm36
'''
from uuid import uuid4

class Message:
    
    def __init__(self, type, fieldDict={}):
        self.fieldDict = fieldDict
        self.type = type
        
    def __setattr__(self, name, value):
        self.fieldDict[name] = value
        
    def __getattr__(self, name):
        return self.fieldDict[name]
    
class JobCompletedMessage:
    
    def __init__(self, jobName):
        self.type = "JOB_COMPLETED"
        self.jobName = jobName
        
class SwitchRootMessage:
    
    def __init__(self, jobName, newRoot):
        self.type = "NEW_ROOT"
        self.jobName = jobName
        self.newRoot = newRoot
        
class QueryRequest(Message):
    
    def __init__(self, requestBody):
        Message.__init__(self, "QUERY")
        self.id = uuid4()
        self.requestBody = requestBody
        
class QueryResponse(Message):
    
    def __init__(self, requestID, responseBody):
        Message.__init__(self, "QUERY_RESPONSE")
        self.requestID = requestID
        self.responseBody = responseBody