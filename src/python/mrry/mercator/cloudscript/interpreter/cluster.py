'''
Created on 15 Apr 2010

@author: dgm36
'''
from mrry.mercator.cloudscript.parser import \
    SWScriptParser
from mrry.mercator.cloudscript.visitors import SkywritingException
from mrry.mercator.runtime.task_executor import SWContinuation
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
        raise SkywritingException("Script did not parse")
    
    cont = SWContinuation(script)
    
    http = httplib2.Http()
    
    master_data_uri = urlparse.urljoin(master_uri, "/data/")
    (response, content) = http.request(master_data_uri, "POST", pickle.dumps(cont))
    continuation_uri = simplejson.loads(content)
    
    print continuation_uri
    
    task_descriptor = {'inputs': {'_cont' : ('urls', [continuation_uri])}, 'handler': 'swi'}
    
    master_task_submit_uri = urlparse.urljoin(master_uri, "/task/")
    (response, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor))
    out = simplejson.loads(content)
    
    print out