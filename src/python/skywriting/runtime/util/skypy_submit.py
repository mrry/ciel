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
from skywriting.runtime.references import SW2_ConcreteReference
from skywriting.runtime.block_store import SWReferenceJSONEncoder,json_decode_object_hook
import time
import datetime
import simplejson
import pickle
import urlparse
import httplib2
import tempfile
import sys
import os
import subprocess
from optparse import OptionParser

def now_as_timestamp():
    return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-i", "--id", action="store", dest="id", help="Job ID", metavar="ID", default="default")
    (options, args) = parser.parse_args()
   
    if not options.master:
        parser.print_help()
        print >> sys.stderr, "Must specify master URI with --master"
        sys.exit(1)

    if len(args) != 1:
        parser.print_help()
        print >> sys.stderr, "Must specify one python file to execute, as argument"
        sys.exit(1)

    master_uri = options.master
    id = options.id
    
    initial_coro_fp, initial_coro_file = tempfile.mkstemp()

    print id, "STARTED", now_as_timestamp()

    script_name = args[0]
    pypy_args = ["/local/scratch/cs448/pypy-1.3/pypy/translator/goal/pypy-c", "/local/scratch/cs448/skywriting/src/python/skywriting/runtime/worker/skypy/stub.py", "--source", script_name]
    pypy_process = subprocess.Popen(pypy_args, stdout=initial_coro_fp)
    os.close(initial_coro_fp)
    pypy_process.wait()
    initial_coro_fp = open(initial_coro_file, "r")
    initial_coro_text = initial_coro_fp.read()
    initial_coro_fp.close()

    pyfile_fp = open(script_name, "r")
    pyfile_text = pyfile_fp.read()
    pyfile_fp.close()

    print id, "FINISHED_PARSING", now_as_timestamp()

    if pypy_process.returncode != 0:
        raise Exception("PyPy failed to parse script")
    
    http = httplib2.Http()
    
    master_data_uri = urlparse.urljoin(master_uri, "/data/")
    (_, content) = http.request(master_data_uri, "POST", initial_coro_text)
    cont_id = simplejson.loads(content)
    (_, content) = http.request(master_data_uri, "POST", pyfile_text)
    pyfile_id = simplejson.loads(content)
    
    print id, "SUBMITTED_CORO_AND_PY", now_as_timestamp()
    
    master_netloc = urlparse.urlparse(master_uri).netloc
    task_descriptor = {'dependencies': {'_coro' : SW2_ConcreteReference(cont_id, len(initial_coro_text), [master_netloc]),
                                        '_py' : SW2_ConcreteReference(pyfile_id, len(pyfile_text), [master_netloc])}, 'handler': 'skypy'}

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
