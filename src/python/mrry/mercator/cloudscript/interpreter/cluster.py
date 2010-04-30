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
import simplejson
import pickle
import urlparse
import httplib2
import sys

if __name__ == '__main__':
    
    print sys.argv
    
    script_name = sys.argv[1]
    master_uri = sys.argv[2]
    
    parser = SWScriptParser()
    
    script = parser.parse(open(script_name, 'r').read())
    
    if script is None:
        print "Script did not parse :("
        exit()
    
    cont = SWContinuation(script, SimpleContext())
    
    http = httplib2.Http()
    
    master_data_uri = urlparse.urljoin(master_uri, "/data/")
    (response, content) = http.request(master_data_uri, "POST", pickle.dumps(cont))
    continuation_uri, size_hint = simplejson.loads(content)
    
    print continuation_uri
    
    task_descriptor = {'inputs': {'_cont' : SWURLReference([continuation_uri], size_hint)}, 'handler': 'swi'}
    
    master_task_submit_uri = urlparse.urljoin(master_uri, "/task/")
    (response, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))
    out = simplejson.loads(content)
    
    print urlparse.urljoin(master_uri, "/global_data/%d" % out[0])