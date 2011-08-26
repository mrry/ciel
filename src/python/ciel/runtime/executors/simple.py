# Copyright (c) 2010-11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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
import hashlib
from ciel.public.references import SW2_FutureReference, SWRealReference
import ciel
import logging
import threading
import datetime
import time
import subprocess
from subprocess import PIPE
from ciel.runtime.executors.base import BaseExecutor
from ciel.runtime.exceptions import BlameUserException
from ciel.runtime.executors import hash_update_with_structure,\
    ContextManager, list_with, add_running_child, remove_running_child
from ciel.runtime.object_cache import ref_from_object,\
    retrieve_object_for_ref
from ciel.runtime.fetcher import retrieve_filenames_for_refs, OngoingFetch
from ciel.runtime.producer import make_local_output

class SimpleExecutor(BaseExecutor):

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, args, n_outputs, is_tail_spawn=False, handler_name=None):

        # This is needed to work around the fact that stdinout has its own implementation of build_task_descriptor, so
        # we can't rely using cls.handler_name to find the actual executor.
        if handler_name is None:
            handler_name = cls.handler_name

        if is_tail_spawn and len(task_descriptor["expected_outputs"]) != n_outputs:
            raise BlameUserException("SimpleExecutor being built with delegated outputs %s but n_outputs=%d" % (task_descriptor["expected_outputs"], n_outputs))

        # Throw early if the args are bad
        cls.check_args_valid(args, n_outputs)

        # Discover required ref IDs for this executor
        reqd_refs = cls.get_required_refs(args)
        task_descriptor["dependencies"].extend(reqd_refs)

        sha = hashlib.sha1()
        hash_update_with_structure(sha, [args, n_outputs])
        name_prefix = "%s:%s:" % (handler_name, sha.hexdigest())

        # Name our outputs
        if not is_tail_spawn:
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]

        # Add the args dict
        args_name = "%ssimple_exec_args" % name_prefix
        args_ref = ref_from_object(args, "pickle", args_name)
        parent_task_record.publish_ref(args_ref)
        task_descriptor["dependencies"].append(args_ref)
        task_descriptor["task_private"]["simple_exec_args"] = args_ref
        
        BaseExecutor.build_task_descriptor(task_descriptor, parent_task_record)

        if is_tail_spawn:
            return None
        else:
            return [SW2_FutureReference(x) for x in task_descriptor["expected_outputs"]]
        
    def resolve_required_refs(self, args):
        try:
            args["inputs"] = [self.task_record.retrieve_ref(ref) for ref in args["inputs"]]
        except KeyError:
            pass

    @classmethod
    def get_required_refs(cls, args):
        required = []
        
        try:
            required.extend([x for x in args["command_line"] if isinstance(x, SWRealReference)])
        except KeyError:
            pass
        
        try:
            # Shallow copy
            required.extend(list(args["inputs"]))
        except KeyError:
            pass
        
        return required

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "inputs" in args:
            for ref in args["inputs"]:
                if not isinstance(ref, SWRealReference):
                    raise BlameUserException("Simple executors need args['inputs'] to be a list of references. %s is not a reference." % ref)

    @staticmethod
    def can_run():
        return True

    def _run(self, task_private, task_descriptor, task_record):
        self.task_record = task_record
        self.task_id = task_descriptor["task_id"]
        self.output_ids = task_descriptor["expected_outputs"]
        self.output_refs = [None for _ in range(len(self.output_ids))]
        self.succeeded = False
        self.args = retrieve_object_for_ref(task_private["simple_exec_args"], "pickle", self.task_record)

        try:
            self.debug_opts = self.args['debug_options']
        except KeyError:
            self.debug_opts = []
        self.resolve_required_refs(self.args)
        try:
            self._execute()
            for ref in self.output_refs:
                if ref is not None:
                    self.task_record.publish_ref(ref)
                else:
                    ciel.log.error("Executor failed to define output %s" % ref.id, "EXEC", logging.WARNING)
            self.succeeded = True
        except:
            ciel.log.error("Task execution failed", "EXEC", logging.ERROR, True)
            raise
        finally:
            self.cleanup_task()
        
    def cleanup_task(self):
        self._cleanup_task()
    
    def _cleanup_task(self):
        pass

class ProcessRunningExecutor(SimpleExecutor):

    def __init__(self, worker):
        SimpleExecutor.__init__(self, worker)

        self._lock = threading.Lock()
        self.proc = None
        self.context_mgr = None

    def _execute(self):
        self.context_mgr = ContextManager("Simple Task %s" % self.task_id)
        with self.context_mgr:
            self.guarded_execute()

    def guarded_execute(self):
        try:
            self.input_refs = self.args['inputs']
        except KeyError:
            self.input_refs = []
        try:
            self.stream_output = self.args['stream_output']
        except KeyError:
            self.stream_output = False
        try:
            self.pipe_output = self.args['pipe_output']
        except KeyError:
            self.pipe_output = False
        try:
            self.eager_fetch = self.args['eager_fetch']
        except KeyError:
            self.eager_fetch = False
        try:
            self.stream_chunk_size = self.args['stream_chunk_size']
        except KeyError:
            self.stream_chunk_size = 67108864

        try:
            self.make_sweetheart = self.args['make_sweetheart']
            if not isinstance(self.make_sweetheart, list):
                self.make_sweetheart = [self.make_sweetheart]
        except KeyError:
            self.make_sweetheart = []

        file_inputs = None
        push_threads = None

        if self.eager_fetch:
            file_inputs = retrieve_filenames_for_refs(self.input_refs, self.task_record)
        else:

            push_threads = [OngoingFetch(ref, chunk_size=self.stream_chunk_size, task_record=self.task_record, must_block=True) for ref in self.input_refs]

            for thread in push_threads:
                self.context_mgr.add_context(thread)

        # TODO: Make these use OngoingOutputs and the context manager.                
        with list_with([make_local_output(id, may_pipe=self.pipe_output) for id in self.output_ids]) as out_file_contexts:

            if self.stream_output:
       
                stream_refs = [ctx.get_stream_ref() for ctx in out_file_contexts]
                self.task_record.prepublish_refs(stream_refs)

            # We do these last, as these are the calls which can lead to stalls whilst we await a stream's beginning or end.
            if file_inputs is None:
                file_inputs = []
                for thread in push_threads:
                    (filename, is_blocking) = thread.get_filename()
                    if is_blocking is not None:
                        assert is_blocking is True
                    file_inputs.append(filename)
            
            file_outputs = [filename for (filename, _) in (ctx.get_filename_or_fd() for ctx in out_file_contexts)]
            
            self.proc = self.start_process(file_inputs, file_outputs)
            add_running_child(self.proc)

            rc = self.await_process(file_inputs, file_outputs)
            remove_running_child(self.proc)

            self.proc = None

            #        if "trace_io" in self.debug_opts:
            #            transfer_ctx.log_traces()

            if rc != 0:
                raise OSError()

        for i, output in enumerate(out_file_contexts):
            self.output_refs[i] = output.get_completed_ref()

        ciel.engine.publish("worker_event", "Executor: Done")

    def start_process(self, input_files, output_files):
        raise Exception("Must override start_process when subclassing ProcessRunningExecutor")
        
    def await_process(self, input_files, output_files):
        rc = self.proc.wait()
        return rc

    def _cleanup_task(self):
        pass

    def _abort(self):
        if self.proc is not None:
            self.proc.kill()
            
class FilenamesOnStdinExecutor(ProcessRunningExecutor):
    
    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

        self.last_event_time = None
        self.current_state = "Starting up"
        self.state_times = dict()

    def change_state(self, new_state):
        time_now = datetime.datetime.now()
        old_state_time = time_now - self.last_event_time
        old_state_secs = float(old_state_time.seconds) + (float(old_state_time.microseconds) / 10**6)
        if self.current_state not in self.state_times:
            self.state_times[self.current_state] = old_state_secs
        else:
            self.state_times[self.current_state] += old_state_secs
        self.last_event_time = time_now
        self.current_state = new_state

    def resolve_required_refs(self, args):
        SimpleExecutor.resolve_required_refs(self, args)
        try:
            args["lib"] = [self.task_record.retrieve_ref(ref) for ref in args["lib"]]
        except KeyError:
            pass

    @classmethod
    def get_required_refs(cls, args):
        l = SimpleExecutor.get_required_refs(args)
        try:
            l.extend(args["lib"])
        except KeyError:
            pass
        return l

    def start_process(self, input_files, output_files):

        try:
            self.argv = self.args['argv']
        except KeyError:
            self.argv = []

        self.before_execute()
        ciel.engine.publish("worker_event", "Executor: running")

        if "go_slow" in self.debug_opts:
            ciel.log.error("DEBUG: Executor sleep(3)'ing", "EXEC", logging.DEBUG)
            time.sleep(3)

        proc = subprocess.Popen(self.get_process_args(), shell=False, stdin=PIPE, stdout=PIPE, stderr=None, close_fds=True)
        self.last_event_time = datetime.datetime.now()
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
                            ciel.engine.publish("worker_event", "Process log %f Computing" % timestamp)
                        elif message[0] == "I":
                            try:
                                params = message[1:].split("|")
                                stream_id = int(params[0])
                                timestamp = float(params[1])
                                ciel.engine.publish("worker_event", "Process log %f Waiting %d" % (timestamp, stream_id))
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
                ciel.log.error("Error gathering I/O trace", "EXEC", logging.DEBUG, True)
                break

    def await_process(self, input_files, output_files):
        self.change_state("Running")
        if "trace_io" in self.debug_opts:
            ciel.log.error("DEBUG: Executor gathering an I/O trace from child", "EXEC", logging.DEBUG)
            self.gather_io_trace()
        rc = self.proc.wait()
        self.change_state("Done")
        ciel.log.error("Process terminated. Stats:", "EXEC", logging.DEBUG)
        for key, value in self.state_times.items():
            ciel.log.error("Time in state %s: %s seconds" % (key, value), "EXEC", logging.DEBUG)
        return rc

    def get_process_args(self):
        raise Exception("Must override get_process_args subclassing FilenamesOnStdinExecutor")
