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
    SWDataValue, SW2_StreamReference
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException, MissingInputException,\
    RuntimeSkywritingError
import logging
import shutil
import subprocess
import tempfile
import os
import cherrypy
import threading
import time
from skywriting.runtime.block_store import STREAM_RETRY
from errno import EPIPE

running_children = {}

def add_running_child(proc):
    running_children[proc.pid] = proc

def remove_running_child(proc):
    del running_children[proc.pid]

def kill_all_running_children():
    for child in running_children.values():
        try:
            child.kill()
            child.wait()
        except:
            pass
        
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
                cherrypy.log.error("Blocking because reference is %s" % repr(ref), 'EXEC', logging.INFO)
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

class ProcessRunningExecutor(SWExecutor):

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.input_refs = args['inputs']
        except KeyError:
            self.input_refs = []
        try:
            self.stream_output = args['stream_output']
        except KeyError:
            self.stream_output = False

        self.proc = None

    def _execute(self, block_store, task_id):
        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        file_outputs = []
        for i in range(len(self.output_refs)):
            with tempfile.NamedTemporaryFile(delete=False) as this_file:
                file_outputs.append(this_file.name)
        
        if self.stream_output:
            stream_refs = {}
            for i, filename in enumerate(file_outputs):
                block_store.prepublish_file(filename, self.output_ids[i])
                stream_ref = SW2_StreamReference(self.output_ids[i])
                stream_ref.add_location_hint(block_store.netloc)
                stream_refs[self.output_ids[i]] = stream_ref
            self.master_proxy.publish_refs(task_id, stream_refs)

        self.proc = self.start_process(block_store, file_inputs, file_outputs, transfer_ctx)
        add_running_child(self.proc)

        rc = self.await_process(block_store, file_inputs, file_outputs, transfer_ctx)
        remove_running_child(self.proc)

        self.proc = None

        cherrypy.engine.publish("worker_event", "Executor: Waiting for transfers (for cache)")
        transfer_ctx.wait_for_all_transfers()
        transfer_ctx.cleanup(block_store)

        failure_bindings = transfer_ctx.get_failed_refs()
        if failure_bindings is not None:
            raise MissingInputException(failure_bindings)

        if rc != 0:
            raise OSError()
        cherrypy.engine.publish("worker_event", "Executor: Storing outputs")
        for i, filename in enumerate(file_outputs):
                    
            if self.stream_output:
                _, size_hint = block_store.commit_file(filename, self.output_ids[i], can_move=True)
            else:
                _, size_hint = block_store.store_file(filename, self.output_ids[i], can_move=True)
            
            # XXX: fix provenance.
            real_ref = SW2_ConcreteReference(self.output_ids[i], size_hint)
            real_ref.add_location_hint(block_store.netloc)
            self.output_refs[i] = real_ref
            
        cherrypy.engine.publish("worker_event", "Executor: Done")

    def start_process(self, block_store, input_files, output_files, transfer_ctx):
        raise Exception("Must override start_process when subclassing ProcessRunningExecutor")
        
    def await_process(self, block_store, input_files, output_files, transfer_ctx):
        rc = self.proc.wait()
        transfer_ctx.consumers_detached()
        return rc

    def _cleanup(self, block_store):
        if self.stream_output and not self.succeeded:
            block_store.rollback_file(self.output_ids[0])
        
    def _abort(self):
        if self.proc is not None:
            self.proc.kill()

class SWStdinoutExecutor(ProcessRunningExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        ProcessRunningExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        if len(expected_output_ids) != 1:
            raise BlameUserException("Stdinout executor must have one output")
        try:
            self.command_line = args['command_line']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))

    def start_process(self, block_store, input_files, output_files, transfer_ctx):
        cherrypy.log.error("Executing stdinout with: %s" % " ".join(map(str, self.command_line)), 'EXEC', logging.INFO)

        with open(output_files[0], "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            return subprocess.Popen(map(str, self.command_line), stdin=PIPE, stdout=temp_output_fp, close_fds=True)

    def await_process(self, block_store, input_files, output_files, transfer_ctx):

        class list_with:
            def __init__(self, l):
                self.wrapped_list = l
            def __enter__(self):
                return [x.__enter__() for x in self.wrapped_list]
            def __exit__(self, exnt, exnv, exntb):
                for x in self.wrapped_list:
                    x.__exit__(exnt, exnv, exntb)
                return False

        with list_with([open(filename, 'r') for filename in input_files]) as fileobjs:
            transfer_ctx.consumers_attached()
            for fileobj in fileobjs:
                try:
                    shutil.copyfileobj(fileobj, self.proc.stdin)
                except IOError, e:
                    if e.errno == EPIPE:
                        cherrypy.log.error('Abandoning cat due to EPIPE', 'EXEC', logging.WARNING)
                        break
                    else:
                        raise

        self.proc.stdin.close()
        rc = self.proc.wait()
        transfer_ctx.consumers_detached()
        return rc
        
class EnvironmentExecutor(ProcessRunningExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        ProcessRunningExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.command_line = args['command_line']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))

    def start_process(self, block_store, input_files, output_files, transfer_ctx):
        cherrypy.log.error("Executing environ with: %s" % " ".join(map(str, self.command_line)), 'EXEC', logging.INFO)

        with tempfile.NamedTemporaryFile(delete=False) as input_filenames_file:
            for filename in input_files:
                input_filenames_file.write(filename)
                input_filenames_file.write('\n')
            input_filenames_name = input_filenames_file.name
            
        with tempfile.NamedTemporaryFile(delete=False) as output_filenames_file:
            for filename in output_files:
                output_filenames_file.write(filename)
                output_filenames_file.write('\n')
            output_filenames_name = output_filenames_file.name
            
        environment = {'INPUT_FILES'  : input_filenames_name,
                       'OUTPUT_FILES' : output_filenames_name}
            
        proc = subprocess.Popen(map(str, self.command_line), env=environment, close_fds=True)

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        transfer_ctx.consumers_attached()
        
        return proc

class FilenamesOnStdinExecutor(ProcessRunningExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        ProcessRunningExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.argv = args['argv']
        except KeyError:
            self.argv = []
        try:
            self.debug_opts = args['debug_options']
        except KeyError:
            self.debug_opts = []
        
    def start_process(self, block_store, input_files, output_files, transfer_ctx):

        self.before_execute(block_store)
        cherrypy.engine.publish("worker_event", "Executor: running")

        if "go_slow" in self.debug_opts:
            cherrypy.log.error("DEBUG: Executor sleep(3)'ing", "EXEC", logging.DEBUG)
            time.sleep(3)

        proc = subprocess.Popen(self.get_process_args(), shell=False, stdin=PIPE, stdout=PIPE, stderr=None, close_fds=True)
        
        proc.stdin.write("%d,%d,%d\0" % (len(input_files), len(output_files), len(self.argv)))
        for x in input_files:
            proc.stdin.write("%s\0" % x)
        for x in output_files:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        transfer_ctx.consumers_attached()

        return proc

    def get_process_args(self):
        raise Exception("Must override get_process_args subclassing FilenamesOnStdinExecutor")

class JavaExecutor(FilenamesOnStdinExecutor):

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        FilenamesOnStdinExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.jar_refs = args['lib']
            self.class_name = args['class']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))

    def before_execute(self, block_store):
        cherrypy.log.error("Running Java executor for class: %s" % self.class_name, "JAVA", logging.INFO)
        cherrypy.engine.publish("worker_event", "Java: fetching JAR")
        self.jar_filenames = self.get_filenames_eager(block_store, self.jar_refs)

    def get_process_args(self):
        cp = os.getenv('CLASSPATH',"/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar")
        process_args = ["java", "-cp", cp, "uk.co.mrry.mercator.task.JarTaskLoader", self.class_name]
        process_args.extend(["file://" + x for x in self.jar_filenames])
        return process_args
        
class DotNetExecutor(FilenamesOnStdinExecutor):

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        FilenamesOnStdinExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.dll_refs = args['lib']
            self.class_name = args['class']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the dotnet executor: %s' % repr(args))

    def before_execute(self, block_store):
        cherrypy.log.error("Running Dotnet executor for class: %s" % self.class_name, "DOTNET", logging.INFO)
        cherrypy.engine.publish("worker_event", "Dotnet: fetching DLLs")
        self.dll_filenames = self.get_filenames_eager(block_store, self.dll_refs)

    def get_process_args(self):

        mono_loader = os.getenv('SW_MONO_LOADER_PATH', 
                                "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/csharp/loader/loader.exe")
        process_args = ["mono", mono_loader, self.class_name]
        process_args.extend(self.dll_filenames)
        return process_args

class CExecutor(FilenamesOnStdinExecutor):

    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        FilenamesOnStdinExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.so_refs = args['lib']
            self.entry_point_name = args['entry_point']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the C executor: %s' % repr(args))

    def before_execute(self, block_store):
        cherrypy.log.error("Running C executor for entry point: %s" % self.entry_point_name, "CEXEC", logging.INFO)
        cherrypy.engine.publish("worker_event", "C-exec: fetching SOs")
        self.so_filenames = self.get_filenames_eager(block_store, self.so_refs)

    def get_process_args(self):

        c_loader = os.getenv('SW_C_LOADER_PATH', 
                             "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/c/src/loader")
        process_args = [c_loader, self.entry_point_name]
        process_args.extend(self.so_filenames)
        return process_args
    
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
            self.output_refs[i] = SWDataValue(self.output_ids[i], ref)
            
class SyncExecutor(SWExecutor):
    
    def __init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit=None):
        SWExecutor.__init__(self, args, continuation, expected_output_ids, master_proxy, fetch_limit)
        try:
            self.inputs = args['inputs']
        except KeyError:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))
        assert len(expected_output_ids) == 1
    
    def _execute(self, block_store, task_id):
        self.output_refs[0] = SWDataValue(self.output_ids[0], True)
