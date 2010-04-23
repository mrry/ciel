'''
Created on 19 Apr 2010

@author: dgm36
'''
from subprocess import PIPE
from mrry.mercator.runtime.references import SWLocalDataFile, SWURLReference,\
    SWDataValue
from mrry.mercator.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException
import shutil
import subprocess
import tempfile

class ExecutionFeatures:
    
    def __init__(self):
        self.executors = {'stdinout': SWStdinoutExecutor,
                          'java': JavaExecutor,
                          'dotnet': DotNetExecutor}
    
    def all_features(self):
        return self.executors.keys()
    
    def get_executor(self, name, args, continuation, num_outputs):
        try:
            Executor = self.executors[name]
        except KeyError:
            raise FeatureUnavailableException(name)
        return Executor(args, continuation, num_outputs)

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
            raise ReferenceUnavailableException(ref, self.continuation)

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
            filename = self.get_filename(block_store, ref)
            print ref, '--->', filename
            with open(filename) as input_file:
                shutil.copyfileobj(input_file, proc.stdin)

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
        SWExecutor.__init__(self, args, continuation, num_outputs)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.jar_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise

    def execute(self, block_store):
        
        file_inputs = map(lambda ref: self.get_filename(block_store, ref), self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        jar_filenames = map(lambda ref: self.get_filename(block_store, ref), self.jar_refs)
        java_stdout = tempfile.NamedTemporaryFile(delete=False)
        java_stderr = tempfile.NamedTemporaryFile(delete=False)

        print "Input filenames:"
        for fn in file_inputs:
            print '\t', fn
        print "Output filenames:"
        for fn in file_outputs:
            print '\t', fn
        
        print 'Stdout:', java_stdout.name, 'Stderr:', java_stderr.name
        
        process_args = ["java", "-cp", "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar", "uk.co.mrry.mercator.task.JarTaskLoader", self.class_name]
        for x in jar_filenames:
            process_args.append("file://" + x)
        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=java_stdout, stderr=java_stderr)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()
        rc = proc.wait()
        print 'Return code', rc
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
        
        urls = map(lambda filename: block_store.store_file(filename), file_outputs)
        url_refs = map(lambda url: SWURLReference([url]), urls)
        self.output_refs = map(lambda url_ref: self.continuation.create_tasklocal_reference(url_ref), url_refs)

class DotNetExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs):
        SWExecutor.__init__(self, args, continuation, num_outputs)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.dll_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise

    def execute(self, block_store):
        
        file_inputs = map(lambda ref: self.get_filename(block_store, ref), self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        dll_filenames = map(lambda ref: self.get_filename(block_store, ref), self.dll_refs)
        dotnet_stdout = tempfile.NamedTemporaryFile(delete=False)
        dotnet_stderr = tempfile.NamedTemporaryFile(delete=False)

        print "Input filenames:"
        for fn in file_inputs:
            print '\t', fn
        print "Output filenames:"
        for fn in file_outputs:
            print '\t', fn
        
        print 'Stdout:', dotnet_stdout.name, 'Stderr:', dotnet_stderr.name
        
        process_args = ["mono", "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/csharp/loader/loader.exe", self.class_name]
        for x in dll_filenames:
            process_args.append(x)
        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=dotnet_stdout, stderr=dotnet_stderr)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()
        rc = proc.wait()
        print 'Return code', rc
        if rc != 0:
            with open(dotnet_stdout.name) as stdout_fp:
                with open(dotnet_stderr.name) as stderr_fp:
                    print ".NET program failed, returning", rc, "with stdout:"
                    for l in stdout_fp.readlines():
                        print l
                    print "...and stderr:"
                    for l in stderr_fp.readlines():
                        print l
            raise OSError()
        
        urls = map(lambda filename: block_store.store_file(filename), file_outputs)
        url_refs = map(lambda url: SWURLReference([url]), urls)
        self.output_refs = map(lambda url_ref: self.continuation.create_tasklocal_reference(url_ref), url_refs)
