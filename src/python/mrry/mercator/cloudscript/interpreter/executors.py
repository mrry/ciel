'''
Created on Apr 11, 2010

@author: derek
'''
import subprocess
from subprocess import PIPE
import tempfile
import urllib2

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
        temp_output_fp = open(temp_output.name, "w")
        print temp_output.name
        proc = subprocess.Popen(self.command_line, stdin=PIPE, stdout=temp_output_fp)
        for input in self.input_refs:
            proc.stdin.write(urllib2.urlopen(input.urls[0]).read())
        proc.stdin.close()
        rc = proc.wait()
        if rc != 0:
            print rc
            raise
        self.output_urls[0] = "file://%s" % (temp_output.name, )