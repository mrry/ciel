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
    SWNoProvenance, SWDataValue, SW2_StreamReference, SWTaskOutputProvenance
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

class SWExecutor:

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        self.continuation = continuation
        self.output_ids = expected_output_ids
        self.output_refs = [None for id in self.output_ids]
        self.fetch_limit = fetch_limit
        self.fetchers = []
        self.master_proxy = master_proxy
        self.succeeded = False

    def make_fetcher(self, block_store, ref):
        fetcher = StreamingInputFetcher(self, block_store, ref)
        fetcher.start()
        self.fetchers.append(fetcher)
        return fetcher.fifo_name
    
    def cleanup_fetchers(self):
        for fetcher in self.fetchers:
            fetcher.stop()
            fetcher.cleanup()

    def get_filename(self, block_store, ref):
        if self.continuation is not None:
            self.continuation.mark_as_execd(ref)
            real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        else:
            real_ref = ref
        print real_ref
        assert isinstance(real_ref, SWRealReference)
        if isinstance(real_ref, SW2_FutureReference):
            print "Blocking because reference is", real_ref
            # Data is not yet available, so 
            raise ReferenceUnavailableException(ref, self.continuation)
        elif isinstance(real_ref, SW2_StreamReference) or isinstance(real_ref, SW2_ConcreteReference):
            cherrypy.log.error('Making stream fetcher for %s' % repr(real_ref), 'EXEC', logging.INFO)
            return self.make_fetcher(block_store, ref)
        else:
            cherrypy.engine.publish("Executor: fetching reference")
            ret = block_store.retrieve_filename_for_ref(real_ref)
            cherrypy.engine.publish("Executor: done fetching reference")
            return ret

    def get_filenames(self, block_store, refs):
        #print "GET_FILENAMES:", refs
        # Mark all as execd before we risk faulting.
        if self.continuation is not None:
            map(self.continuation.mark_as_execd, refs)
        return map(lambda ref: self.get_filename(block_store, ref), refs)
        
    def execute(self, block_store, task_id):
        try:
            self._execute(block_store, task_id)
            self.succeeded = True
        except:
            raise
        finally:
            self.cleanup(block_store)
        
    def cleanup(self, block_store):
        self._cleanup(block_store)
        self.cleanup_fetchers()
    
    def _cleanup(self, block_store):
        pass
        
    def abort(self):
        self._abort()
        
    def _abort(self):
        pass

class StreamingInputFetcher:
    '''
    This class implements the chunked fetching of SW2_StreamReferences and 
    SW2_ConcreteReferences from other nodes.
    '''
    
    def __init__(self, executor, block_store, stream_ref):
        self.executor = executor
        self.block_store = block_store
        self.ref = stream_ref
        
        if self.block_store.is_available_locally(stream_ref):
            self.local = True
            self.fifo_name = self.block_store.retrieve_filename_for_concrete_ref(stream_ref)
        else:
            self.local = False
            self.fifo_dir = tempfile.mkdtemp()
            self.fifo_name = os.path.join(self.fifo_dir, 'fetch_fifo')
            
            with tempfile.NamedTemporaryFile(delete=False) as sinkfile:
                self.sinkfile_name = sinkfile.name
            
            os.mkfifo(self.fifo_name)
            self.stop_event = threading.Event()
            self.has_completed = False
            self.thread = None
            
            self.current_start_byte = 0
            self.chunk_size = 1024768
        
    def start(self):
        if not self.local:
            self.thread = threading.Thread(target=self.fetch_thread_main)
            self.thread.start()
    
    def stop(self):
        if not self.local:
            self.stop_event.set()
            if self.thread is not None:
                self.thread.join()
            
    def cleanup(self):
        if not self.local:
            os.unlink(self.fifo_name)
            os.rmdir(self.fifo_dir)
            if self.has_completed:
                self.block_store.store_file(self.sinkfile_name, self.ref.id, True)
        
    def fetch_thread_main(self):
        assert not self.local
        with open(self.fifo_name, 'wb') as f:
            with open(self.sinkfile_name, 'wb') as f_sink:
                while not self.has_completed:
                    
                    # Try to fetch a chunk.
                    try:
                        # TODO: Make chunk size adaptive.
                        # XXX: The chunk size might be bigger than the FIFO buffer.
                        #      Need to ensure that we don't block badly.
                        chunk = self.block_store.retrieve_range_for_ref(self.ref, self.current_start_byte, self.chunk_size)
                        if not chunk:
                            cherrypy.log.error('Completed fetching ref %s' % (repr(self.ref)), 'FETCHER', logging.INFO)
                            self.has_completed = True
                            return
                        elif chunk is not STREAM_RETRY:
                            cherrypy.log.error('Fetched %d bytes of ref %s' % (len(chunk), repr(self.ref)), 'FETCHER', logging.INFO)
                            f.write(chunk)
                            f_sink.write(chunk)
                            self.current_start_byte += len(chunk)
                            
                    except:
                        cherrypy.log.error('Error attempting to read a range', 'FETCHER', logging.WARNING, True)
                        self.executor.abort()
                        return
                              
                    # We use self.stop_event to signal this fetcher that its 
                    # executor is stopping/has failed.  
                    if self.stop_event.is_set():
                        return
                    
                    if chunk is STREAM_RETRY:
                        # TODO: Make wait period adaptive.
                        if self.stop_event.wait(5):
                            return
                    
    
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
    
    def _execute(self, block_store, task_id):
        print "Executing stdinout with:", " ".join(map(str, self.command_line))
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        filenames = self.get_filenames(block_store, self.input_refs)
        
        if self.stream_output:
            block_store.prepublish_file(temp_output.name, self.output_ids[0])
            stream_ref = SW2_StreamReference(self.output_ids[0], SWTaskOutputProvenance(task_id, 0))
            stream_ref.add_location_hint(block_store.netloc)
            self.master_proxy.publish_refs(task_id, {self.output_ids[0] : stream_ref})
        
        with open(temp_output.name, "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            self.proc = subprocess.Popen(map(str, self.command_line), stdin=PIPE, stdout=temp_output_fp)
    
        try:
            for filename in filenames:
                with open(filename, 'r') as input_file:
                    shutil.copyfileobj(input_file, self.proc.stdin)
        except IOError:
            # We may get a broken pipe if the executor finishes early, so we
            # let this exception pass, and determine success from the return value.
            pass

        self.proc.stdin.close()
        rc = self.proc.wait()
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
            self.proc.wait()

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

        input_filenames = self.get_filenames(block_store, self.input_refs)
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

        rc = self.proc.wait()
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
        file_inputs = self.get_filenames(block_store, self.input_refs)
        file_outputs = [tempfile.NamedTemporaryFile().name for i in range(len(self.output_refs))]
        
        cherrypy.engine.publish("worker_event", "Java: fetching JAR")
        jar_filenames = map(lambda ref: self.get_filename(block_store, ref), self.jar_refs)

        if self.stream_output:
            stream_refs = {}
            for i, filename, id in enumerate(file_outputs):
                block_store.prepublish_file(filename, self.output_ids[i])
                stream_ref = SW2_StreamReference(self.output_ids[i], SWTaskOutputProvenance(task_id, i))
                stream_ref.add_location_hint(block_store.netloc)
                stream_refs[id] = stream_ref
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
