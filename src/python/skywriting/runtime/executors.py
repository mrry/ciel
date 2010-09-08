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
from subprocess import PIPE
from skywriting.runtime.references import \
    SWRealReference, SW2_FutureReference, SW2_ConcreteReference,\
    SWNoProvenance, SWDataValue, SW2_StreamReference
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException
import logging
import shutil
import subprocess
import tempfile
import os
import cherrypy
import threading
from skywriting.runtime.block_store import STREAM_RETRY

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = {'swi': None, # Could make a special SWI interpreter....
                          'stdinout': SWStdinoutExecutor,
                          'environ': EnvironmentExecutor,
                          'java': JavaExecutor,
                          'dotnet': DotNetExecutor,
                          'c': CExecutor,
                          'grab': GrabURLExecutor}
    
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

    def resolve_ref(self, ref):
        if self.continuation is not None:
            self.continuation.mark_as_execd(ref)
            return self.continuation.resolve_tasklocal_reference_with_ref(ref)
        else:
            return ref        

    def mark_and_test_refs(self, refs):
        # Mark all as execd before we risk faulting.
        if self.continuation is not None:
            map(self.continuation.mark_as_execd, refs)

        real_refs = map(self.resolve_ref, refs)
        for ref in real_refs:
            assert isinstance(ref, SWRealReference)
            if isinstance(ref, SW2_FutureReference):
                print "Blocking because reference is", real_ref
                # Data is not yet available, so 
                raise ReferenceUnavailableException(ref, self.continuation)
        return real_refs

    def get_filenames(self, block_store, refs):
        real_refs = self.mark_and_test_refs(refs)
        return block_store.retrieve_filenames_for_refs(real_refs)

    def get_filenames_eager(self, block_store, refs):
        real_refs = self.mark_and_test_refs(refs)
        return block_store.retrieve_filenames_for_refs_eager(real_refs)

    def get_filename(self, block_store, ref):
        files, ctx = self.get_filenames(block_store, [ref])
        return (files[0], ctx)
        
    def execute(self, block_store, task_id):
        try:
            self._execute(block_store, task_id)
        finally:
            self.cleanup()
        
    def cleanup(self):
        self._cleanup()
    
    def _cleanup(self):
        pass
        
    def abort(self):
        self._abort()
        
    def _abort(self):
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

    class CatThread:
        def __init__(self, filenames, stdin):
            self.filenames = filenames
            self.stdin = stdin
            pass

        def start(self):
            self.thread = threading.Thread(target=self.cat_thread_main)
            self.thread.start()
            
        def cat_thread_main(self):
            for filename in self.filenames:
                print 'Catting in', filename
                with open(filename, 'r') as input_file:
                    shutil.copyfileobj(input_file, self.stdin)
            self.stdin.close()

    def _execute(self, block_store, task_id):
        print "Executing stdinout with:", " ".join(map(str, self.command_line))
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        filenames, fetch_ctx = self.get_filenames(block_store, self.input_refs)
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            self.proc = subprocess.Popen(map(str, self.command_line), stdin=PIPE, stdout=temp_output_fp)

        cat_thread = self.CatThread(filenames, self.proc.stdin)
        cat_thread.start()

        fetch_ctx.transfer_all()

        rc = self.proc.wait()

        fetch_ctx.cleanup(block_store)

        if rc != 0:
            print rc
            raise OSError()
        
        _, size_hint = block_store.store_file(temp_output.name, self.output_ids[0], can_move=True)
        
        # XXX: We fix the provenance in the caller.
        real_ref = SW2_ConcreteReference(self.output_ids[0], SWNoProvenance(), size_hint)
        real_ref.add_location_hint(block_store.netloc)
        self.output_refs[0] = real_ref
        
    def _abort(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()

class EnvironmentExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, fetch_limit)
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))
        self.proc = None
    
    def _execute(self, block_store, task_id):
        print "Executing environ with:", " ".join(map(str, self.command_line))

        input_filenames, fetch_ctx = self.get_filenames(block_store, self.input_refs)
        with tempfile.NamedTemporaryFile(delete=False) as input_filenames_file:
            for filename in input_filenames:
                input_filenames_file.write(filename)
                input_filenames_file.write('\n')
            input_filenames_name = input_filenames_file.name
            
        output_filenames = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        with tempfile.NamedTemporaryFile(delete=False) as output_filenames_file:
            for filename in output_filenames:
                output_filenames_file.write(filename)
                output_filenames_file.write('\n')
            output_filenames_name = output_filenames_file.name
            
        environment = {'INPUT_FILES'  : input_filenames_name,
                       'OUTPUT_FILES' : output_filenames_name}
            
        self.proc = subprocess.Popen(map(str, self.command_line), env=environment)

        fetch_ctx.transfer_all()

        rc = self.proc.wait()
        
        fetch_ctx.cleanup(block_store)

        if rc != 0:
            print rc
            raise OSError()
        for i, filename in enumerate(output_filenames):
            _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            # XXX: We fix the provenance in the caller.
            real_ref = SW2_ConcreteReference(self.output_ids[i], SWNoProvenance(), size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref
        
    def _abort(self):
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

    def _execute(self, block_store, task_id):
        cherrypy.engine.publish("worker_event", "Java: fetching inputs")
        print "Get fifos"
        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        print "Got fifos"
        file_outputs = [tempfile.NamedTemporaryFile().name for i in range(len(self.output_refs))]
        
        cherrypy.engine.publish("worker_event", "Java: fetching JAR")
        print "Jar fetch"
        jar_filenames = self.get_filenames_eager(block_store, self.jar_refs)
        print "Jar fetch done"

#        print "Input filenames:"
#        for fn in file_inputs:
#            print '\t', fn
#        print "Output filenames:"
#        for fn in file_outputs:
#            print '\t', fn

        cherrypy.engine.publish("worker_event", "Java: running")

        #print 'Stdout:', java_stdout.name, 'Stderr:', java_stderr.name
        cp = os.getenv('CLASSPATH',"/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar")
        process_args = ["java", "-cp", cp, "uk.co.mrry.mercator.task.JarTaskLoader", self.class_name]
        for x in jar_filenames:
            process_args.append("file://" + x)
        print 'Command-line:', " ".join(process_args)
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=None, stderr=None)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()

        transfer_ctx.transfer_all()

        rc = proc.wait()
#        print 'Return code', rc
        transfer_ctx.cleanup(block_store)

        cherrypy.engine.publish("worker_event", "Java: JVM finished")
        if rc != 0:
            raise OSError()
        cherrypy.engine.publish("worker_event", "Java: Storing outputs")
        for i, filename in enumerate(file_outputs):
            _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            # XXX: fix provenance.
            real_ref = SW2_ConcreteReference(self.output_ids[i], SWNoProvenance(), size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref
        cherrypy.engine.publish("worker_event", "Java: Finished storing outputs")

    def _abort(self):
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

    def _execute(self, block_store, task_id):
        
        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        dll_filenames = self.get_filenames_eager(block_store, self.dll_refs)
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

        transfer_ctx.transfer_all()

        rc = proc.wait()
        print 'Return code', rc

        transfer_ctx.cleanup(block_store)

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
            _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            # XXX: fix provenance.
            real_ref = SW2_ConcreteReference(self.output_ids[i], SWNoProvenance(), size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref

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

    def _execute(self, block_store, task_id):
        
        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile(delete=False).name for i in range(len(self.output_refs))]
        
        so_filenames = self.get_filenames_eager(block_store, self.so_refs)
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

        transfer_ctx.transfer_all()

        rc = proc.wait()
        print 'Return code', rc

        transfer_ctx.cleanup(block_store)

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
            _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            # XXX: fix provenance.
            real_ref = SW2_ConcreteReference(self.output_ids[i], SWNoProvenance(), size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref
            
class GrabURLExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, fetch_limit)
        try:
            self.urls = args['urls']
            self.version = args['version']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))
        assert len(self.urls) == len(expected_output_ids)
    
    def _execute(self, block_store, task_id):
        cherrypy.log.error('Starting to fetch URLs', 'FETCHEXECUTOR', logging.INFO)
        
        for i, url in enumerate(self.urls):
            ref = block_store.get_ref_for_url(url, self.version, task_id)
            self.output_refs[i] = SWDataValue(ref)
