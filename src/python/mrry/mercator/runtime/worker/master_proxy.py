'''
Created on 15 Apr 2010

@author: dgm36
'''
import httplib2

import simplejson

class MasterProxy:
    
    def __init__(self, master_netloc=None):
        self.master_netloc = master_netloc
        self.http = httplib2.Http()
    
    def change_master(self, master_details):
        self.master_netloc = master_details['netloc']
        
    def get_master_details(self):
        return {'netloc': self.master_netloc}
    
    def publish_global_object(self, global_id, urls):
        message_payload = simplejson.dumps({global_id: urls})
        message_url = "http://%s/global_data/%d" % (self.master_netloc, global_id)
        self.http.request(message_url, "POST", message_payload)
        
    def spawn_tasks(self, parent_task_id, tasks):
        message_payload = simplejson.dumps(tasks)
        message_url = "http://%s/task/%d/spawn" % (self.master_netloc, parent_task_id)
        (_, result) = self.http.request(message_url, "POST", message_payload)
        return simplejson.loads(result)
    
    def commit_task(self, task_id, bindings):
        message_payload = simplejson.dumps(bindings)
        message_url = "http://%s/task/%d/commit" % (self.master_netloc, task_id)
        self.http.request(message_url, "POST", message_payload)
        
    def failed_task(self, task_id):
        message_payload = simplejson.dumps(task_id)
        message_url = "http://%s/task/%d/failed" % (self.master_netloc, task_id)
        self.http.request(message_url, "POST", message_payload)
