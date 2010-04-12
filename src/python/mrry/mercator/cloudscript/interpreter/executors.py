'''
Created on Apr 11, 2010

@author: derek
'''
from threading import Lock
import subprocess
from subprocess import PIPE
import tempfile
import urllib2

SUBPROCESS_LOCK = Lock()

class StdinoutExecutor:
    
    def __init__(self, args, num_outputs):
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise
        
        self.output_urls = [None]
    
    def execute(self):
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        print temp_output.name
        
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            with SUBPROCESS_LOCK:
                proc = subprocess.Popen(self.command_line, stdin=PIPE, stdout=temp_output_fp)
        
        proc.communicate("".join([urllib2.urlopen(x.urls[0]).read() for x in self.input_refs]))
        print "About to wait..."
        rc = proc.wait()
        if rc != 0:
            print rc
            raise OSError()
        self.output_urls[0] = "file://%s" % (temp_output.name, )