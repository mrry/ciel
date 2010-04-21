'''
Created on 19 Apr 2010

@author: dgm36
'''
from subprocess import PIPE
from mrry.mercator.runtime.references import SWLocalDataFile, SWURLReference,\
    SWDataValue
from mrry.mercator.cloudscript.visitors import ExecutionInterruption
import urllib2
import shutil
import subprocess
import tempfile

class SWExecutor:

    def __init__(self, args, continuation, num_outputs):
        self.continuation = continuation
        self.output_refs = [None for i in range(num_outputs)]

    def get_filename(self, block_store, ref):
        self.continuation.mark_as_execd(ref)
        real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        if isinstance(real_ref, SWLocalDataFile):
            return real_ref.filename
        elif isinstance(real_ref, SWURLReference):
            return block_store.retrieve_filename_by_url(real_ref.urls[0])
        elif isinstance(real_ref, SWDataValue):
            return block_store.retrieve_filename_by_url(block_store.store_object(real_ref.value, 'json'))
        else:
            # Data is not yet available, so 
            raise ExecutionInterruption()

class SWStdinoutExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs):
        SWExecutor.__init__(self, args, continuation, num_outputs)
        assert num_outputs == 1
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise
    
    def execute(self, block_store):
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            proc = subprocess.Popen(self.command_line, stdin=PIPE, stdout=temp_output_fp)
    
        for ref in self.input_refs:
            shutil.copyfileobj(open(self.get_filename(block_store, ref), 'r'), proc.stdin)

        proc.stdin.close()
        rc = proc.wait()
        if rc != 0:
            print rc
            raise OSError()
        
        url = block_store.store_file(temp_output.name)
        real_ref = SWURLReference([url])
        tasklocal_ref = self.continuation.create_tasklocal_reference(real_ref)
        self.output_refs[0] = tasklocal_ref

class JavaExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs):
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.jar_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise
        self.output_refs = [None for i in num_outputs]

    def execute(self, block_store):
        
        file_inputs = map(lambda ref: self.get_filename(block_store, ref), self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in len(self.output_refs)]
        
        # TODO: implement communication with JVM.
        
        urls = map(lambda filename: block_store.store_file(filename), file_outputs)
        url_refs = map(lambda url: SWURLReference[url], urls)
        self.output_refs = map(lambda url_ref: self.continuation.create_tasklocal_reference(url_ref))
