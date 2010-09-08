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
from __future__ import with_statement
from skywriting.runtime.task_executor import SWContinuation,\
    SWRuntimeInterpreterTask
import httplib2
import urlparse
import pickle
import simplejson
from skywriting.runtime.block_store import SWReferenceJSONEncoder,\
    json_decode_object_hook, sw_to_external_url
from skywriting.runtime.references import SWDataValue, SWURLReference

'''
Created on 17 May 2010

@author: dgm36
'''

from skywriting.lang.context import SimpleContext
from skywriting.lang.parser import SWStatementParser
import cmd

class SWInteractiveShell(cmd.Cmd):
    
    def __init__(self, master_uri):
        cmd.Cmd.__init__(self)
        self.master_uri = master_uri
        self.local_continuation = SWContinuation(None, SimpleContext())
        self.stmt_parser = SWStatementParser()

    
    def dereference_task_result(self, result_ref):
        if isinstance(result_ref, SWDataValue):
            return result_ref.value
        else:
            print result_ref
            return "Too unusual to return!"
    
    def submit_task_and_wait(self, task_stmt):
        
        # 1. Update the local continuation with the current task_stmt.
        self.local_continuation.task_stmt = task_stmt
        
        # 2. POST the updated local continuation.
        http = httplib2.Http()
        master_data_uri = urlparse.urljoin(self.master_uri, "/data/")
        (_, content) = http.request(master_data_uri, "POST", pickle.dumps(self.local_continuation))
        continuation_uri, size_hint = simplejson.loads(content)
        
        # 3. Submit a new task with the updated local continuation.
        task_descriptor = {'dependencies': {'_cont' : SWURLReference([continuation_uri], size_hint)}, 'handler': 'swi', 'save_continuation': True}
        master_task_submit_uri = urlparse.urljoin(self.master_uri, "/task/")
        (_, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))
        submit_result = simplejson.loads(content)
        
        # 4. Block to get the final result.
        expected_output_id = submit_result['outputs'][0]
        notify_url = urlparse.urljoin(self.master_uri, "/global_data/%d/completion" % expected_output_id)
        (_, result_content) = http.request(notify_url)
        completion_result = simplejson.loads(result_content, object_hook=json_decode_object_hook)
        if completion_result["exited"]:
            raise Exception("Server exited")

        # 5. Get updated local continuation. N.B. The originally-spawned task may have delegated, so we need to find the task from the actual producer of the expected output.
        task_for_output_url = urlparse.urljoin(self.master_uri, "/global_data/%d/task" % expected_output_id)
        (_, content) = http.request(task_for_output_url, "GET")
        end_task_descriptor = simplejson.loads(content, object_hook=json_decode_object_hook)
        saved_continuation_url = sw_to_external_url(end_task_descriptor['saved_continuation_uri'])
        (_, content) = http.request(saved_continuation_url)
        self.local_continuation = pickle.loads(content)
        
        # 6. Dereference result.
        return self.dereference_task_result(completion_result.refs[0])
        
    def do_print(self, arg):
        task_stmt = self.stmt_parser.parse("return %s;" % arg)
        if self.local_continuation.task_stmt is None:
            self.stmt_parser.parser.restart()
            print "Syntax error"
            return
            
        result = self.submit_task_and_wait(task_stmt)
      
        print result
        
        return False
    
    def do_exit(self, arg):
        return True
    
    def do_quit(self, arg):
        return True
    
    def do_EOF(self, arg):
        print
        return True
    
    def default(self, line):
        task_stmt = self.stmt_parser.parse(line)
        if task_stmt is None:
            self.stmt_parser.parser.restart()
            print "Syntax error"
            return
        result = self.submit_task_and_wait(task_stmt)
        print result
        return False

def interactive_main(options):
    shell = SWInteractiveShell(options.master)
    shell.cmdloop()
