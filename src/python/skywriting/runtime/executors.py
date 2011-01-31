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
from shared.references import \
    SWRealReference, SW2_FutureReference, SW2_ConcreteReference,\
    SWDataValue, SW2_StreamReference
from skywriting.runtime.references import SWReferenceJSONEncoder
from skywriting.runtime.exceptions import FeatureUnavailableException,\
    ReferenceUnavailableException, BlameUserException, MissingInputException,\
    RuntimeSkywritingError
import simplejson
import logging
import shutil
import subprocess
import tempfile
import os
import os.path
import cherrypy
import threading
import time
import codecs
from datetime import datetime
from skywriting.runtime.block_store import STREAM_RETRY
from errno import EPIPE
import ciel

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
                          'skypy': None, # And again...
                          'stdinout': SWStdinoutExecutor,
                          'environ': EnvironmentExecutor,
                          'java': JavaExecutor,
                          'dotnet': DotNetExecutor,
                          'c': CExecutor,
                          'grab': GrabURLExecutor,
                          'sync': SyncExecutor}
    
    def all_features(self):
        return self.executors.keys()
    
    def get_executor(self, name):
        try:
            Executor = self.executors[name]
        except KeyError:
            raise FeatureUnavailableException(name)
        return Executor()

class SWExecutor:

    def __init__(self):
        pass

    def resolve_ref(self, ref, continuation):
        continuation.mark_as_execd(ref)
        return continuation.resolve_tasklocal_reference_with_ref(ref)

    def resolve_required_refs(self, foreign_args, get_ref_callback):
        try:
            foreign_args["inputs"] = [get_ref_callback(ref) for ref in foreign_args["inputs"]]
        except KeyError:
            pass

    def get_required_refs(self, foreign_args, required_callback):
        try:
            for input in foreign_args["inputs"]:
                required_callback(input)
        except KeyError:
            pass

    def check_args_valid(self, args, expected_output_ids):
        pass

    def get_filenames(self, block_store, refs):
        # Refs should already have been tested.
        return block_store.retrieve_filenames_for_refs(refs, "trace_io" in self.debug_opts)

    def get_filenames_eager(self, block_store, refs):
        return block_store.retrieve_filenames_for_refs_eager(refs)

    def get_filename(self, block_store, ref):
        files, ctx = self.get_filenames(block_store, [ref])
        return (files[0], ctx)

    def execute(self, block_store, task_id, args, expected_output_ids, publish_callback, master_proxy, fetch_limit=None):
        # On entry: args have been checked for validity and their relevant refs resolved to consumable references (i.e. Concrete, Streaming, DataValue).
        self.output_ids = expected_output_ids
        self.output_refs = [None for i in range(len(self.output_ids))]
        self.fetch_limit = fetch_limit
        self.master_proxy = master_proxy
        self.succeeded = False
        self.args = args
        self.publish_callback = publish_callback
        try:
            self.debug_opts = args['debug_options']
        except KeyError:
            self.debug_opts = []
        try:
            self._execute(block_store, task_id)
            for ref in self.output_refs:
                if ref is not None:
                    publish_callback(ref)
                else:
                    ciel.log.error("Executor failed to define output %s" % ref.id, "EXEC", logging.WARNING)
            self.succeeded = True
        except:
            ciel.log.error("Task execution failed", "EXEC", logging.ERROR, True)
            raise
        finally:
            self.cleanup(block_store)
        
    def cleanup(self, block_store):
        self._cleanup(block_store)
    
    def _cleanup(self, block_store):
        pass
    
    def notify_streams_done(self):
        # Out-of-thread call
        # Overridden for process-running executors
        pass
        
    def abort(self):
        self._abort()
        
    def _abort(self):
        pass

class ProcessRunningExecutor(SWExecutor):

    def __init__(self):
        SWExecutor.__init__(self)

        self._lock = threading.Lock()
        self.proc = None
        self.transfer_ctx = None

    def notify_streams_done(self):
        # Out-of-thread call. Indicates our streaming inputs, if any, have all finished.
        # We should retry stream-fetches right away.
        with self._lock:
            if self.transfer_ctx is not None:
                self.transfer_ctx.notify_streams_done()

    def _execute(self, block_store, task_id):
        try:
            self.input_refs = self.args['inputs']
        except KeyError:
            self.input_refs = []
        try:
            self.stream_output = self.args['stream_output']
        except KeyError:
            self.stream_output = False

        file_inputs, transfer_ctx = self.get_filenames(block_store, self.input_refs)
        with self._lock:
            self.transfer_ctx = transfer_ctx
        file_outputs = []
        for i in range(len(self.output_ids)):
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
        if "trace_io" in self.debug_opts:
            transfer_ctx.log_traces()

        transfer_ctx.cleanup(block_store)

        failure_bindings = transfer_ctx.get_failed_refs()
        if failure_bindings is not None:
            raise MissingInputException(failure_bindings)

        if rc != 0:
            raise OSError()
        cherrypy.engine.publish("worker_event", "Executor: Storing outputs")
        for i, filename in enumerate(file_outputs):

            file_size = os.path.getsize(filename)
            if file_size < 1024 and not self.stream_output:
                with open(filename, "r") as f:
                    # DataValues must be ASCII so the JSON encoder won't explode.
                    # Decoding gets done in the block store's retrieve routines.
                    encoder = codecs.lookup("string_escape")
                    real_ref = SWDataValue(self.output_ids[i], (encoder.encode(f.read()))[0])
            else:
                if self.stream_output:
                    block_store.commit_file(filename, self.output_ids[i], can_move=True)
                else:
                    block_store.store_file(filename, self.output_ids[i], can_move=True)
                real_ref = SW2_ConcreteReference(self.output_ids[i], file_size)
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
    
    def __init__(self):
        ProcessRunningExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):

        if len(expected_output_ids) != 1:
            raise BlameUserException("Stdinout executor must have one output")
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))

    def start_process(self, block_store, input_files, output_files, transfer_ctx):

        command_line = self.args["command_line"]
        ciel.log.error("Executing stdinout with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.INFO)

        with open(output_files[0], "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            return subprocess.Popen(map(str, command_line), stdin=PIPE, stdout=temp_output_fp, close_fds=True)

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
                        ciel.log.error('Abandoning cat due to EPIPE', 'EXEC', logging.WARNING)
                        break
                    else:
                        raise

        self.proc.stdin.close()
        rc = self.proc.wait()
        transfer_ctx.consumers_detached()
        return rc
        
class EnvironmentExecutor(ProcessRunningExecutor):
    
    def __init__(self):
        ProcessRunningExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):

        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))

    def start_process(self, block_store, input_files, output_files, transfer_ctx):

        command_line = self.args["command_line"]
        ciel.log.error("Executing environ with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.INFO)

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
            
        proc = subprocess.Popen(map(str, command_line), env=environment, close_fds=True)

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        transfer_ctx.consumers_attached()
        
        return proc

class FilenamesOnStdinExecutor(ProcessRunningExecutor):
    
    def __init__(self):
        ProcessRunningExecutor.__init__(self)

        self.last_event_time = None
        self.current_state = "Starting up"
        self.state_times = dict()

    def change_state(self, new_state):
        time_now = datetime.now()
        old_state_time = time_now - self.last_event_time
        old_state_secs = float(old_state_time.seconds) + (float(old_state_time.microseconds) / 10**6)
        if self.current_state not in self.state_times:
            self.state_times[self.current_state] = old_state_secs
        else:
            self.state_times[self.current_state] += old_state_secs
        self.last_event_time = time_now
        self.current_state = new_state

    def resolve_required_refs(self, foreign_args, get_ref_callback):
        SWExecutor.resolve_required_refs(self, foreign_args, get_ref_callback)
        try:
            foreign_args["lib"] = [get_ref_callback(ref) for ref in foreign_args["lib"]]
        except KeyError:
            pass

    def get_required_refs(self, foreign_args, required_callback):
        SWExecutor.get_required_refs(self, foreign_args, required_callback)
        try:
            for lib in foreign_args["lib"]:
                required_callback(lib)
        except KeyError:
            pass

    def start_process(self, block_store, input_files, output_files, transfer_ctx):

        try:
            self.argv = self.args['argv']
        except KeyError:
            self.argv = []

        self.before_execute(block_store)
        cherrypy.engine.publish("worker_event", "Executor: running")

        if "go_slow" in self.debug_opts:
            ciel.log.error("DEBUG: Executor sleep(3)'ing", "EXEC", logging.DEBUG)
            time.sleep(3)

        proc = subprocess.Popen(self.get_process_args(), shell=False, stdin=PIPE, stdout=PIPE, stderr=None, close_fds=True)
        self.last_event_time = datetime.now()
        self.change_state("Writing input details")
        
        proc.stdin.write("%d,%d,%d\0" % (len(input_files), len(output_files), len(self.argv)))
        for x in input_files:
            proc.stdin.write("%s\0" % x)
        for x in output_files:
            proc.stdin.write("%s\0" % x)
        for x in self.argv:
            proc.stdin.write("%s\0" % x)
        proc.stdin.close()
        self.change_state("Waiting for FIFO pickup")

        _ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        transfer_ctx.consumers_attached()

        return proc

    def gather_io_trace(self):
        anything_read = False
        while True:
            try:
                message = ""
                while True:
                    c = self.proc.stdout.read(1)
                    if not anything_read:
                        self.change_state("Gathering IO trace")
                        anything_read = True
                    if c == ",":
                        if message[0] == "C":
                           timestamp = float(message[1:])
                           cherrypy.engine.publish("worker_event", "Process log %f Computing" % timestamp)
                        elif message[0] == "I":
                            try:
                                params = message[1:].split("|")
                                stream_id = int(params[0])
                                timestamp = float(params[1])
                                cherrypy.engine.publish("worker_event", "Process log %f Waiting %d" % (timestamp, stream_id))
                            except:
                                ciel.log.error("Malformed data from stdout: %s" % message)
                                raise
                        else:
                            ciel.log.error("Malformed data from stdout: %s" % message)
                            raise Exception("Malformed stuff")
                        break
                    elif c == "":
                        raise Exception("Stdout closed")
                    else:
                        message = message + c
            except Exception as e:
                print e
                break

    def await_process(self, block_store, input_files, output_files, transfer_ctx):
        self.change_state("Running")
        if "trace_io" in self.debug_opts:
            ciel.log.error("DEBUG: Executor gathering an I/O trace from child", "EXEC", logging.INFO)
            self.gather_io_trace()
        rc = self.proc.wait()
        self.change_state("Done")
        transfer_ctx.consumers_detached()
        ciel.log.error("Process terminated. Stats:", "EXEC", logging.INFO)
        for key, value in self.state_times.items():
            ciel.log.error("Time in state %s: %s seconds" % (key, value), "EXEC", logging.INFO)
        return rc

    def get_process_args(self):
        raise Exception("Must override get_process_args subclassing FilenamesOnStdinExecutor")

class JavaExecutor(FilenamesOnStdinExecutor):

    def __init__(self):
        FilenamesOnStdinExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):

        if "lib" not in args or "class" not in args:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))

    def before_execute(self, block_store):

        self.jar_refs = self.args["lib"]
        self.class_name = self.args["class"]
        ciel.log.error("Running Java executor for class: %s" % self.class_name, "JAVA", logging.INFO)
        cherrypy.engine.publish("worker_event", "Java: fetching JAR")
        self.jar_filenames = self.get_filenames_eager(block_store, self.jar_refs)

    def get_process_args(self):
        cp = os.getenv('CLASSPATH',"/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/JavaBindings.jar")
        process_args = ["java", "-cp", cp]
        if "trace_io" in self.debug_opts:
            process_args.append("-Dskywriting.trace_io=1")
        process_args.extend(["uk.co.mrry.mercator.task.JarTaskLoader", self.class_name])
        process_args.extend(["file://" + x for x in self.jar_filenames])
        return process_args
        
class DotNetExecutor(FilenamesOnStdinExecutor):

    def __init__(self):
        FilenamesOnStdinExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):

        if "lib" not in args or "class" not in args:
            raise BlameUserException('Incorrect arguments to the dotnet executor: %s' % repr(args))

    def before_execute(self, block_store):

        self.dll_refs = self.args['lib']
        self.class_name = self.args['class']

        ciel.log.error("Running Dotnet executor for class: %s" % self.class_name, "DOTNET", logging.INFO)
        cherrypy.engine.publish("worker_event", "Dotnet: fetching DLLs")
        self.dll_filenames = self.get_filenames_eager(block_store, self.dll_refs)

    def get_process_args(self):

        mono_loader = os.getenv('SW_MONO_LOADER_PATH', 
                                "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/csharp/loader/loader.exe")
        process_args = ["mono", mono_loader, self.class_name]
        process_args.extend(self.dll_filenames)
        return process_args

class CExecutor(FilenamesOnStdinExecutor):

    def __init__(self):
        FilenamesOnStdinExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):

        if "lib" not in args or "entry_point" not in args:
            raise BlameUserException('Incorrect arguments to the C-so executor: %s' % repr(args))

    def before_execute(self, block_store):
        self.so_refs = args['lib']
        self.entry_point_name = args['entry_point']
        ciel.log.error("Running C executor for entry point: %s" % self.entry_point_name, "CEXEC", logging.INFO)
        cherrypy.engine.publish("worker_event", "C-exec: fetching SOs")
        self.so_filenames = self.get_filenames_eager(block_store, self.so_refs)

    def get_process_args(self):

        c_loader = os.getenv('SW_C_LOADER_PATH', 
                             "/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/c/src/loader")
        process_args = [c_loader, self.entry_point_name]
        process_args.extend(self.so_filenames)
        return process_args
    
class GrabURLExecutor(SWExecutor):
    
    def __init__(self):
        SWExecutor.__init__(self)
    
    def check_args_valid(self, args, expected_output_ids):
        
        if "urls" not in args or "version" not in args or len(args["urls"]) != len(expected_output_ids):
            raise BlameUserException('Incorrect arguments to the grab executor: %s' % repr(args))

    def _execute(self, block_store, task_id):

        urls = self.args['urls']
        version = self.args['version']

        ciel.log.error('Starting to fetch URLs', 'FETCHEXECUTOR', logging.INFO)
        
        for i, url in enumerate(urls):
            ref = block_store.get_ref_for_url(url, version, task_id)
            self.publish_callback(ref)
            out_str = simplejson.dumps(ref, cls=SWReferenceJSONEncoder)
            block_store.cache_object(ref, "json", self.output_ids[i])
            self.output_refs[i] = SWDataValue(self.output_ids[i], out_str)

        ciel.log.error('Done fetching URLs', 'FETCHEXECUTOR', logging.INFO)
            
class SyncExecutor(SWExecutor):
    
    def __init__(self):
        SWExecutor.__init__(self)

    def check_args_valid(self, args, expected_output_ids):
        if "inputs" not in args or len(expected_output_ids) != 1:
            raise BlameUserException('Incorrect arguments to the sync executor: %s' % repr(self.args))            

    def _execute(self, block_store, task_id):
        self.inputs = self.args['inputs']
        block_store.cache_object(True, "json", self.output_ids[0])
        self.output_refs[0] = SWDataValue(self.output_ids[0], simplejson.dumps(True))
