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
from skywriting.lang.parser import \
    SWScriptParser
from skywriting.runtime.task_executor import SWContinuation
from skywriting.runtime.references import SW2_ConcreteReference
from skywriting.runtime.block_store import SWReferenceJSONEncoder,json_decode_object_hook
from skywriting.lang.context import SimpleContext
import time
import datetime
import simplejson
import pickle
import urlparse
import httplib2
import sys
import os
from optparse import OptionParser

def now_as_timestamp():
    return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-i", "--id", action="store", dest="id", help="Job ID", metavar="ID", default="default")
    parser.add_option("-e", "--env", action="store_true", dest="send_env", help="Set this flag to send the current environment with the script as _env", default=False)
    (options, args) = parser.parse_args()
   
    if not options.master:
        parser.print_help()
        print >> sys.stderr, "Must specify master URI with --master"
        sys.exit(1)

    if len(args) != 1:
        parser.print_help()
        print >> sys.stderr, "Must specify one script file to execute, as argument"
        sys.exit(1)

    script_name = args[0]
    master_uri = options.master
    id = options.id
    
    print id, "STARTED", now_as_timestamp()

    parser = SWScriptParser()
    
    script = parser.parse(open(script_name, 'r').read())

    print id, "FINISHED_PARSING", now_as_timestamp()
    
    if script is None:
        print "Script did not parse :("
        exit()
    
    cont = SWContinuation(script, SimpleContext())
    if options.send_env:
        cont.context.bind_identifier('env', os.environ)
    
    http = httplib2.Http()
    
    master_data_uri = urlparse.urljoin(master_uri, "/data/")
    pickled_cont = pickle.dumps(cont)
    (_, content) = http.request(master_data_uri, "POST", pickled_cont)
    cont_id = simplejson.loads(content)
    
    out_id = 'joboutput:%s' % cont_id
    
    print id, "SUBMITTED_CONT", now_as_timestamp()
    
    #print continuation_uri
    
    master_netloc = urlparse.urlparse(master_uri).netloc
    task_descriptor = {'dependencies': {'_cont' : SW2_ConcreteReference(cont_id, len(pickled_cont), [master_netloc])}, 'handler': 'swi', 'expected_outputs' : [out_id]}
    
    master_task_submit_uri = urlparse.urljoin(master_uri, "/job/")
    (_, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))
    
    print id, "SUBMITTED_JOB", now_as_timestamp() 
    
    
    out = simplejson.loads(content)
    
    notify_url = urlparse.urljoin(master_uri, "/job/%s/completion" % out['job_id'])
    job_url = urlparse.urljoin(master_uri, "/browse/job/%s" % out['job_id'])

    print id, "JOB_URL", job_url
    
    #print "Blocking to get final result"
    (_, content) = http.request(notify_url)
    completion_result = simplejson.loads(content, object_hook=json_decode_object_hook)
    if "error" in completion_result.keys():
        print id, "ERROR", completion_result["error"]
        return None
    else:
        print id, "GOT_RESULT", now_as_timestamp()
        #print content
        return completion_result["result_ref"]

if __name__ == '__main__':
    main()
