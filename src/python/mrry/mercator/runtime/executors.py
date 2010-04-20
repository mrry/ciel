'''
Created on 19 Apr 2010

@author: dgm36
'''
from subprocess import PIPE
from mrry.mercator.runtime.references import SWLocalDataFile, SWURLReference
import urllib2
import shutil
import subprocess
import tempfile

class SWStdinoutExecutor:
    
    def __init__(self, args, continuation, num_outputs):
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise
        self.output_refs = [None]
    
    def execute(self, block_store):
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            proc = subprocess.Popen(self.command_line, stdin=PIPE, stdout=temp_output_fp)
    
        for ref in self.input_refs:
            real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
            if isinstance(real_ref, SWLocalDataFile):
                with open(real_ref.filename, 'r') as local_file:
                    shutil.copyfileobj(local_file, proc.stdin)
            elif isinstance(real_ref, SWURLReference):
                shutil.copyfileobj(urllib2.urlopen(real_ref.urls[0]), proc.stdin)

        proc.stdin.close()
        rc = proc.wait()
        if rc != 0:
            print rc
            raise OSError()
        
        url = block_store.store_file(temp_output.name)
        real_ref = SWURLReference([url])
        tasklocal_ref = self.continuation.create_tasklocal_reference(real_ref)
        self.output_refs[0] = tasklocal_ref
