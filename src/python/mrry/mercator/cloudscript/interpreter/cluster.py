'''
Created on 15 Apr 2010

@author: dgm36
'''
from mrry.mercator.cloudscript.parser import \
    SWScriptParser
from mrry.mercator.runtime.task_executor import SWContinuation
from mrry.mercator.runtime.references import SWURLReference
from mrry.mercator.runtime.block_store import SWReferenceJSONEncoder
from mrry.mercator.cloudscript.context import SimpleContext
import time
import datetime
import simplejson
import pickle
import urlparse
import httplib2
import sys

def now_as_timestamp():
    return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

if __name__ == '__main__':
    
    print sys.argv
    
    script_name = sys.argv[1]
    master_uri = sys.argv[2]
    id = sys.argv[3]
    
    print id, "STARTED", now_as_timestamp()

    
    parser = SWScriptParser()
    
    script = parser.parse(open(script_name, 'r').read())

    print id, "FINISHED_PARSING", now_as_timestamp()
    
    if script is None:
        print "Script did not parse :("
        exit()
    
    cont = SWContinuation(script, SimpleContext())
    
    http = httplib2.Http()
    
    master_data_uri = urlparse.urljoin(master_uri, "/data/")
    (response, content) = http.request(master_data_uri, "POST", pickle.dumps(cont))
    continuation_uri, size_hint = simplejson.loads(content)
    
    print id, "SUBMITTED_CONT", now_as_timestamp()
    
    #print continuation_uri
    
    task_descriptor = {'inputs': {'_cont' : SWURLReference([continuation_uri], size_hint)}, 'handler': 'swi'}
    
    master_task_submit_uri = urlparse.urljoin(master_uri, "/task/")
    (response, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))
    
    print id, "SUBMITTED_JOB", now_as_timestamp() 
    
    
    out = simplejson.loads(content)
    gd_url = urlparse.urljoin(master_uri, "/global_data/%d" % out[0])
    notify_url = urlparse.urljoin(master_uri, "/global_data/%d/completion" % out[0])

    print id, "GLOBAL_DATA_URL", gd_url
    
    #print "Blocking to get final result"
    (response, content) = http.request(notify_url)
    print id, "GOT_RESULT", now_as_timestamp()
    
    #print content
