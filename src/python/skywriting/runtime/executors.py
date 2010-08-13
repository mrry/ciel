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
Created on 19 Apr 2010

@author: dgm36
'''
from __future__ import with_statement
from subprocess import PIPE
from skywriting.runtime.references import SWLocalDataFile, SWURLReference,\
    SWDataValue
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException
import shutil
import subprocess
import tempfile
import os

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = {'swi': None, # Could make a special SWI interpreter....
                          'stdinout': SWStdinoutExecutor,
                          'java': JavaExecutor,
                          'dotnet': DotNetExecutor,
                          'c': CExecutor}
    
    def all_features(self):
        return self.executors.keys()
    
    def get_executor(self, name, args, continuation, expected_output_ids, fetch_limit=None):
        try:
            Executor = self.executors[name]
        except KeyError:
            raise FeatureUnavailableException(name)
        return Executor(args, continuation, expected_output_ids, fetch_limit)

class SWExecutor:

    def __init__(self, args, continuation, expected_output_ids, fetch_limit=None):
        self.continuation = continuation
        self.output_ids = expected_output_ids
        self.output_refs = [None for id in self.output_ids]
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
            return block_store.retrieve_filename_by_url(block_store.store_object(real_ref.value, 'json', block_store.allocate_new_id())[0])
        elif isinstance(real_ref, list):
            raise BlameUserException("Attempted to exec with invalid argument: %s" % repr(real_ref))
        else:
            print "Blocking because reference is", real_ref
            # Data is not yet available, so 
            raise ReferenceUnavailableException(ref, self.continuation)

    def get_filenames(self, block_store, refs):
        #print "GET_FILENAMES:", refs
        # Mark all as execd before we risk faulting.
        if self.continuation is not None:
            map(self.continuation.mark_as_execd, refs)
        return map(lambda ref: self.get_filename(block_store, ref), refs)
        
    def abort(self):
        pass
        
class SWStdinoutExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, fetch_limit)
        assert len(expected_output_ids) == 1
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
        
        url, size_hint = block_store.store_file(temp_output.name, self.output_ids[0], can_move=True)
        real_ref = SWURLReference([url], size_hint)
        self.output_refs[0] = real_ref
        
    def abort(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()

class JavaExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, fetch_limit)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.jar_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))

    def execute(self, block_store):
        file_inputs = self.get_filenames(block_store, self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile().name for i in range(len(self.output_refs))]
        
        jar_filenames = map(lambda ref: self.get_filename(block_store, ref), self.jar_refs)

#        print "Input filenames:"
#        for fn in file_inputs:
#            print '\t', fn
#        print "Output filenames:"
#        for fn in file_outputs:
#            print '\t', fn
        
        #print 'Stdout:', java_stdout.name, 'Stderr:', java_stderr.name
        cp = os.getenv('CLASSPATH',"/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar")
        process_args = ["java", "-cp", cp, "uk.co.skywriting.task.JarTaskLoader", self.class_name]
        for x in jar_filenames:
            process_args.append("file://" + x)
#        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=None, stderr=None)
        
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
            raise OSError()
        for i, filename in enumerate(file_outputs):
            url, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            url_ref = SWURLReference([url], size_hint)
            self.output_refs[i] = url_ref

    def abort(self):
        if self.proc is not None:
            self.proc.kill()
        
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
        
        for i, filename in enumerate(file_outputs):
            url, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            url_ref = SWURLReference([url], size_hint)
            self.output_refs[i] = url_ref

class CExecutor(SWExecutor):
    
    def __init__(self, args, continuation, num_outputs):
        SWExecutor.__init__(self, args, continuation, num_outputs)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.so_refs = args['lib']
            self.entry_point_name = args['entry_point']
            self.argv = args['argv']
        except KeyError:
            print "Incorrect arguments for stdinout executor"
            raise

    def execute(self, block_store):
        
        file_inputs = map(lambda ref: self.get_filename(block_store, ref), self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        so_filenames = map(lambda ref: self.get_filename(block_store, ref), self.so_refs)
        c_stdout = tempfile.NamedTemporaryFile(delete=False)
        c_stderr = tempfile.NamedTemporaryFile(delete=False)

        print "Input filenames:"
        for fn in file_inputs:
            print '\t', fn
        print "Output filenames:"
        for fn in file_outputs:
            print '\t', fn
        
        print 'Stdout:', c_stdout.name, 'Stderr:', c_stderr.name
        
        process_args = ["/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/c/src/loader", self.entry_point_name]
        for x in so_filenames:
            process_args.append(x)
        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=c_stdout, stderr=c_stderr)
        
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
            with open(c_stdout.name) as stdout_fp:
                with open(c_stderr.name) as stderr_fp:
                    print "C program failed, returning", rc, "with stdout:"
                    for l in stdout_fp.readlines():
                        print l
                    print "...and stderr:"
                    for l in stderr_fp.readlines():
                        print l
            raise OSError()
        
        for i, filename in enumerate(file_outputs):
            url, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            url_ref = SWURLReference([url], size_hint)
            self.output_refs[i] = url_ref
