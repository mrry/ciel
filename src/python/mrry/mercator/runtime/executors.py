'''
Created on 19 Apr 2010

@author: dgm36
'''
from subprocess import PIPE
from mrry.mercator.runtime.references import SWLocalDataFile, SWURLReference,\
    SWDataValue
from mrry.mercator.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException
import shutil
import subprocess
import tempfile
import os

class ExecutionFeatures:
    
    def __init__(self):
        self.executors = {'swi': None, # Could make a special SWI interpreter....
                          'stdinout': SWStdinoutExecutor,
                          'java': JavaExecutor}
    
    def all_features(self):
        return self.executors.keys()
    
    def get_executor(self, name, args, continuation, num_outputs, fetch_limit=None):
        try:
            Executor = self.executors[name]
        except KeyError:
            raise FeatureUnavailableException(name)
        return Executor(args, continuation, num_outputs, fetch_limit)

class SWExecutor:

    def __init__(self, args, continuation, num_outputs, fetch_limit=None):
        self.continuation = continuation
        self.output_refs = [None for i in range(num_outputs)]
        self.fetch_limit = fetch_limit

    def get_filename(self, block_store, ref):
        if self.continuation is not None:
            self.continuation.mark_as_execd(ref)
            real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        else:
            real_ref = ref
            
        if isinstance(real_ref, SWLocalDataFile):
            return real_ref.filename
        elif isinstance(real_ref, SWURLReference):
            return block_store.retrieve_filename_by_url(block_store.choose_best_url(real_ref.urls), self.fetch_limit)
        elif isinstance(real_ref, SWDataValue):
            return block_store.retrieve_filename_by_url(block_store.store_object(real_ref.value, 'json')[0])
        elif isinstance(real_ref, list):
            raise BlameUserException("Attempted to exec with invalid argument: %s", repr(real_ref))
        else:
            print "Blocking because reference is", real_ref
            # Data is not yet available, so 
            raise ReferenceUnavailableException(ref, self.continuation)

    def get_filenames(self, block_store, refs):
        print "GET_FILENAMES:", refs
        # Mark all as execd before we risk faulting.
        if self.continuation is not None:
            map(self.continuation.mark_as_execd, refs)
        return map(lambda ref: self.get_filename(block_store, ref), refs)
        
    def abort(self):
        pass
        
class SWStdinoutExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, num_outputs, fetch_limit)
        assert num_outputs == 1
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))
        self.proc = None
    
    def execute(self, block_store):
        print "Executing stdinout with:", " ".join(map(str, self.command_line))
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        filenames = self.get_filenames(block_store, self.input_refs)
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            self.proc = subprocess.Popen(map(str, self.command_line), stdin=PIPE, stdout=temp_output_fp)
    
        for filename in filenames:
            with open(filename, 'r') as input_file:
                shutil.copyfileobj(input_file, self.proc.stdin)

        self.proc.stdin.close()
        rc = self.proc.wait()
        if rc != 0:
            print rc
            raise OSError()
        
        url, size_hint = block_store.store_file(temp_output.name, can_move=True)
        real_ref = SWURLReference([url], size_hint)
        self.output_refs[0] = real_ref
        
    def abort(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()

class JavaExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, num_outputs, fetch_limit)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.jar_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))

    def execute(self, block_store):
        file_inputs = self.get_filenames(block_store, self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        jar_filenames = map(lambda ref: self.get_filename(block_store, ref), self.jar_refs)
        java_stdout = tempfile.NamedTemporaryFile(delete=False)
        java_stderr = tempfile.NamedTemporaryFile(delete=False)

#        print "Input filenames:"
#        for fn in file_inputs:
#            print '\t', fn
#        print "Output filenames:"
#        for fn in file_outputs:
#            print '\t', fn
#        
#        print 'Stdout:', java_stdout.name, 'Stderr:', java_stderr.name
        cp = os.getenv('CLASSPATH',"/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar")
        process_args = ["java", "-cp", cp, "uk.co.mrry.mercator.task.JarTaskLoader", self.class_name]
        for x in jar_filenames:
            process_args.append("file://" + x)
#        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=java_stdout, stderr=java_stderr) # Shell=True to find Java in our path
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()
        rc = proc.wait()
#        print 'Return code', rc
        if rc != 0:
            with open(java_stdout.name) as stdout_fp:
                with open(java_stderr.name) as stderr_fp:
                    print "Java program failed, returning", rc, "with stdout:"
                    for l in stdout_fp.readlines():
                        print l
                    print "...and stderr:"
                    for l in stderr_fp.readlines():
                        print l
            raise OSError()
        
        for i, filename in enumerate(file_outputs):
            url, size_hint = block_store.store_file(filename, can_move=True)
            url_ref = SWURLReference([url], size_hint)
            self.output_refs[i] = url_ref

    def abort(self):
        if self.proc is not None:
            self.proc.kill()
