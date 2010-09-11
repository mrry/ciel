# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#                    Chris Smowton <chris.smowton@cl.cam.ac.uk>
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
    SWNoProvenance, SWDataValue, SW2_StreamReference, SWTaskOutputProvenance
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException, MissingInputException
import logging
import shutil
import subprocess
import tempfile
import os
import cherrypy
import threading
from skywriting.runtime.block_store import STREAM_RETRY
from errno import EPIPE

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = {'swi': None, # Could make a special SWI interpreter....
                          'stdinout': SWStdinoutExecutor,
                          'environ': EnvironmentExecutor,
                          'java': JavaExecutor,
                          'dotnet': DotNetExecutor,
                          'c': CExecutor,
                          'grab': GrabURLExecutor,
                          'sync': SyncExecutor}
    
    def all_features(self):
        return self.executors.keys()
    
    def get_executor(self, name, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        try:
            Executor = self.executors[name]
        except KeyError:
            raise FeatureUnavailableException(name)
        return Executor(args, continuation, expected_output_ids, master_proxy, fetch_limit)

class ProcessWaiter:

    def __init__(self, proc, pipefd):
        self.proc = proc
        self.pipefd = pipefd
        self.has_terminated = False
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.lock)
        self.thread = threading.Thread(target=self.waiter_main)
        self.thread.start()

    def waiter_main(self):
        rc = self.proc.wait()
        print "Process terminated; returned", rc
        os.write(self.pipefd, "X")
        with self.lock:
            self.has_terminated = True
            self.return_code = rc
            self.condvar.notify_all()

    def wait(self):
        with self.lock:
            while not self.has_terminated:
                self.condvar.wait()
            return self.return_code

class SWExecutor:

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        self.continuation = continuation
        self.output_ids = expected_output_ids
        self.output_refs = [None for id in self.output_ids]
        self.fetch_limit = fetch_limit
        self.master_proxy = master_proxy
        self.succeeded = False

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
                print "Blocking because reference is", ref
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
            self.succeeded = True
        except:
            cherrypy.log.error("Task execution failed", "EXEC", logging.ERROR, True)
            raise
        finally:
            self.cleanup(block_store)
        
    def cleanup(self, block_store):
        self._cleanup(block_store)
    
    def _cleanup(self, block_store):
        pass
        
    def abort(self):
        self._abort()
        
    def _abort(self):
        pass

class SWStdinoutExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        assert len(expected_output_ids) == 1
        try:
            self.input_refs = args['inputs']
            self.command_line = args['command_line']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))
        
        try:
            self.stream_output = args['stream_output']
        except KeyError:
            self.stream_output = False
        
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
                    try:
                        shutil.copyfileobj(input_file, self.stdin)
                    except IOError, e:
                        if e.errno == EPIPE:
                            print "Abandoning cat due to EPIPE"
                            pass
                        else:
                            raise
            self.stdin.close()

    def _execute(self, block_store, task_id):
        print "Executing stdinout with:", " ".join(map(str, self.command_line))
        with tempfile.NamedTemporaryFile(delete=False) as temp_output:
            temp_output_name = temp_output.name

        filenames, fetch_ctx = self.get_filenames(block_store, self.input_refs)
        
        if self.stream_output:
            block_store.prepublish_file(temp_output_name, self.output_ids[0])
            stream_ref = SW2_StreamReference(self.output_ids[0], SWTaskOutputProvenance(task_id, 0))
            stream_ref.add_location_hint(block_store.netloc)
            self.master_proxy.publish_refs(task_id, {self.output_ids[0] : stream_ref})
        
        with open(temp_output_name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            self.proc = subprocess.Popen(map(str, self.command_line), stdin=PIPE, stdout=temp_output_fp, close_fds=True)
    
        cat_thread = self.CatThread(filenames, self.proc.stdin)
        cat_thread.start()

        read_pipe, write_pipe = os.pipe()

        self.waiter_thread = ProcessWaiter(self.proc, write_pipe)

        fetch_ctx.transfer_all(read_pipe)

        rc = self.waiter_thread.wait()

        fetch_ctx.cleanup(block_store)

        os.close(read_pipe)
        os.close(write_pipe)

        failure_bindings = fetch_ctx.get_failed_refs()
        if failure_bindings is not None:
            raise MissingInputException(failure_bindings)

        self.proc = None

        if rc != 0:
            print rc
            raise OSError()
        
        if self.stream_output:
            _, size_hint = block_store.commit_file(temp_output.name, self.output_ids[0], can_move=True)
        else:
            _, size_hint = block_store.store_file(temp_output.name, self.output_ids[0], can_move=True)
        
        # XXX: We fix the provenance in the caller.
        real_ref = SW2_ConcreteReference(self.output_ids[0], SWNoProvenance(), size_hint)
        real_ref.add_location_hint(block_store.netloc)
        self.output_refs[0] = real_ref
        
    def _cleanup(self, block_store):
        if self.stream_output and not self.succeeded:
            block_store.rollback_file(self.output_ids[0])
        
    def _abort(self):
        if self.proc is not None:
            self.proc.kill()
            self.waiter_thread.wait()

class EnvironmentExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
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
            
        output_filenames = []
        with tempfile.NamedTemporaryFile(delete=False) as output_filenames_file:
            for _ in self.output_refs:
                with tempfile.NamedTemporaryFile(delete=False) as this_file:
                    output_filenames.append(this_file.name)
                    output_filenames_file.write(this_file.name)
                    output_filenames_file.write('\n')
            output_filenames_name = output_filenames_file.name
            
        environment = {'INPUT_FILES'  : input_filenames_name,
                       'OUTPUT_FILES' : output_filenames_name}
            
        self.proc = subprocess.Popen(map(str, self.command_line), env=environment, close_fds=True)

        read_pipe, write_pipe = os.pipe()
        self.wait_thread = ProcessWaiter(self.proc, write_pipe)

        fetch_ctx.transfer_all(read_pipe)

        rc = self.wait_thread.wait()
        
        fetch_ctx.cleanup(block_store)

        os.close(read_pipe)
        os.close(write_pipe)

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
            self.wait_thread.wait()

class JavaExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        self.continuation = continuation
        try:
            self.input_refs = args['inputs']
            self.jar_refs = args['lib']
            self.class_name = args['class']
            self.argv = args['argv']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))
        
        try:
            self.stream_output = args['stream_output']
        except KeyError:
            self.stream_output = False
        
    def _execute(self, block_store, task_id):
        cherrypy.engine.publish("worker_event", "Java: fetching inputs")
        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        file_outputs = []
        for i in range(len(self.output_refs)):
            with tempfile.NamedTemporaryFile(delete=False) as this_file:
                file_outputs.append(this_file.name)
        
        cherrypy.engine.publish("worker_event", "Java: fetching JAR")
        jar_filenames = self.get_filenames_eager(block_store, self.jar_refs)

        if self.stream_output:
            stream_refs = {}
            for i, filename in enumerate(file_outputs):
                block_store.prepublish_file(filename, self.output_ids[i])
                stream_ref = SW2_StreamReference(self.output_ids[i], SWTaskOutputProvenance(task_id, i))
                stream_ref.add_location_hint(block_store.netloc)
                stream_refs[self.output_ids[i]] = stream_ref
            self.master_proxy.publish_refs(task_id, stream_refs)

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
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=None, stderr=None, close_fds=True)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()

        read_pipe, write_pipe = os.pipe()
        waiter_thread = ProcessWaiter(proc, write_pipe)

        transfer_ctx.transfer_all(read_pipe)

        rc = waiter_thread.wait()
#        print 'Return code', rc
        transfer_ctx.cleanup(block_store)

        os.close(read_pipe)
        os.close(write_pipe)

        failure_bindings = transfer_ctx.get_failed_refs()
        if failure_bindings is not None:
            raise MissingInputException(failure_bindings)
            
        cherrypy.engine.publish("worker_event", "Java: JVM finished")
        if rc != 0:
            raise OSError()
        cherrypy.engine.publish("worker_event", "Java: Storing outputs")
        for i, filename in enumerate(file_outputs):
                    
            if self.stream_output:
                _, size_hint = block_store.commit_file(filename, self.output_ids[i], can_move=True)
            else:
                _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            
            # XXX: fix provenance.
            real_ref = SW2_ConcreteReference(self.output_ids[i], SWNoProvenance(), size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref
            
        cherrypy.engine.publish("worker_event", "Java: Finished storing outputs")

    def _cleanup(self, block_store):
        if self.stream_output and not self.succeeded:
            map(block_store.rollback_file, self.output_ids)

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
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=dotnet_stdout, stderr=dotnet_stderr, close_fds=True)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()

        read_pipe, write_pipe = os.pipe()
        waiter_thread = ProcessWaiter(proc, write_pipe)

        transfer_ctx.transfer_all(read_pipe)

        rc = waiter_thread.wait()
        print 'Return code', rc

        transfer_ctx.cleanup(block_store)

        os.close(read_pipe)
        os.close(write_pipe)

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
        
        proc = subprocess.Popen(process_args, shell=False, stdin=PIPE, stdout=c_stdout, stderr=c_stderr, close_fds=True)
        
        proc.stdin.write("%d,%d,%d\0" % (len(file_inputs), len(file_outputs), len(self.argv)))
        for x in file_inputs:
            proc.stdin.write("%s\0" % x)
        for x in file_outputs:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()

        read_pipe, write_pipe = os.pipe()
        waiter_thread = ProcessWaiter(proc, write_pipe)

        transfer_ctx.transfer_all(read_pipe)

        rc = waiter_thread.wait()
        print 'Return code', rc

        transfer_ctx.cleanup(block_store)

        os.close(read_pipe)
        os.close(write_pipe)

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
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
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
            
class SyncExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.inputs = args['inputs']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))
        assert len(expected_output_ids) == 1
    
    def _execute(self, block_store, task_id):
        self.output_refs[0] = SWDataValue(True)
