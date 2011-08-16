# Copyright (c) 2010--11 Derek Murray <derek.murray@cl.cam.ac.uk>
#                        Chris Smowton <chris.smowton@cl.cam.ac.uk>
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
import threading
import ciel
import os
from ciel.public.references import SW2_ConcreteReference, SW2_SweetheartReference,\
    SW2_FixedReference, SW2_FutureReference, SWErrorReference
import pickle
from ciel.runtime.executors.base import BaseExecutor
from ciel.runtime.producer import write_fixed_ref_string, ref_from_string,\
    ref_from_safe_string
from ciel.runtime.block_store import get_own_netloc
from ciel.runtime.exceptions import BlameUserException, TaskFailedError,\
    MissingInputException, ReferenceUnavailableException
from ciel.runtime.executors import ContextManager,\
    spawn_task_helper, OngoingOutput, package_lookup
import subprocess
from ciel.public.io_helpers import write_framed_json, read_framed_json
import logging
from ciel.runtime.fetcher import retrieve_filename_for_ref,\
    retrieve_file_or_string_for_ref, OngoingFetch
import datetime
import time
import socket
import struct

try:
    import sendmsg
    sendmsg_enabled = True
except ImportError:
    sendmsg_enabled = False

never_reuse_process = False
def set_never_reuse_process(setting=True):
    global never_reuse_process
    ciel.log("Disabling process reuse", "PROC", logging.INFO)
    never_reuse_process = setting

# Return states for proc task termination.
PROC_EXITED = 0
PROC_MUST_KEEP = 1
PROC_MAY_KEEP = 2
PROC_ERROR = 3

class ProcExecutor(BaseExecutor):
    """Executor for running generic processes."""
    
    handler_name = "proc"
    
    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)
        self.process_pool = worker.process_pool
        self.ongoing_fetches = []
        self.ongoing_outputs = dict()
        self.transmit_lock = threading.Lock()

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, 
                              process_record_id=None, is_fixed=False, command=None, proc_pargs=[], proc_kwargs={}, force_n_outputs=None,
                              n_extra_outputs=0, extra_dependencies=[], is_tail_spawn=False, accept_ref_list_for_single=False):

        #if process_record_id is None and start_command is None:
        #    raise BlameUserException("ProcExecutor tasks must specify either process_record_id or start_command")

        if process_record_id is not None:
            task_descriptor["task_private"]["id"] = process_record_id
        if command is not None:
            task_descriptor["task_private"]["command"] = command
        task_descriptor["task_private"]["proc_pargs"] = proc_pargs
        task_descriptor["task_private"]["proc_kwargs"] = proc_kwargs
        task_descriptor["dependencies"].extend(extra_dependencies)

        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        if is_fixed:
            task_private_ref = SW2_FixedReference(task_private_id, get_own_netloc())
            write_fixed_ref_string(pickle.dumps(task_descriptor["task_private"]), task_private_ref)
        else:
            task_private_ref = ref_from_string(pickle.dumps(task_descriptor["task_private"]), task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)

        if force_n_outputs is not None:        
            if "expected_outputs" in task_descriptor and len(task_descriptor["expected_outputs"]) > 0:
                raise BlameUserException("Task already had outputs, but force_n_outputs is set")
            task_descriptor["expected_outputs"] = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(force_n_outputs)]
        
        if not is_tail_spawn:
            if len(task_descriptor["expected_outputs"]) == 1 and not accept_ref_list_for_single:
                return SW2_FutureReference(task_descriptor["expected_outputs"][0])
            else:
                return [SW2_FutureReference(refid) for refid in task_descriptor["expected_outputs"]]

    def get_command(self):
        raise TaskFailedError("Attempted to get_command() for an executor that does not define this.")
    
    def get_env(self):
        return {}

    @staticmethod
    def can_run():
        return True
    
    def _run(self, task_private, task_descriptor, task_record):
        
        with ContextManager("Task %s" % task_descriptor["task_id"]) as manager:
            self.context_manager = manager
            self._guarded_run(task_private, task_descriptor, task_record)
            
    def _guarded_run(self, task_private, task_descriptor, task_record):
        
        self.task_record = task_record
        self.task_descriptor = task_descriptor
        self.expected_outputs = list(self.task_descriptor['expected_outputs'])
        
        self.error_details = None
        
        if "id" in task_private:
            id = task_private['id']
            self.process_record = self.process_pool.get_process_record(id)
        else:
            self.process_record = self.process_pool.get_soft_cache_process(self.__class__, task_descriptor["dependencies"])
            if self.process_record is None:
                self.process_record = self.process_pool.create_process_record(None, "json")
                if "command" in task_private:
                    command = [task_private["command"]]
                else:
                    command = self.get_command()
                command.extend(["--write-fifo", self.process_record.get_read_fifo_name(), 
                                "--read-fifo", self.process_record.get_write_fifo_name()])
                new_proc_env = os.environ.copy()
                new_proc_env.update(self.get_env())
                new_proc = subprocess.Popen(command, env=new_proc_env, close_fds=True)
                self.process_record.set_pid(new_proc.pid)
               
        # XXX: This will block until the attached process opens the pipes.
        reader = self.process_record.get_read_fifo()
        writer = self.process_record.get_write_fifo()
        self.reader = reader
        self.writer = writer
        
        #ciel.log('Got reader and writer FIFOs', 'PROC', logging.INFO)

        write_framed_json(("start_task", task_private), writer)

        try:
            if self.process_record.protocol == 'line':
                finished = self.line_event_loop(reader, writer)
            elif self.process_record.protocol == 'json':
                finished = self.json_event_loop(reader, writer)
            else:
                raise BlameUserException('Unsupported protocol: %s' % self.process_record.protocol)
        
        except MissingInputException, mie:
            self.process_pool.delete_process_record(self.process_record)
            raise
            
        except TaskFailedError, tfe:
            finished = PROC_ERROR
            self.error_details = tfe.message
            
        except:
            ciel.log('Got unexpected error', 'PROC', logging.ERROR, True)
            finished = PROC_ERROR
        
        global never_reuse_process
        if finished == PROC_EXITED or never_reuse_process:
            self.process_pool.delete_process_record(self.process_record)
        
        elif finished == PROC_MAY_KEEP:
            self.process_pool.soft_cache_process(self.process_record, self.__class__, self.soft_cache_keys)    
        
        elif finished == PROC_MUST_KEEP:
            pass
        elif finished == PROC_ERROR:
            ciel.log('Task died with an error', 'PROC', logging.ERROR)
            for output_id in self.expected_outputs:
                task_record.publish_ref(SWErrorReference(output_id, 'RUNTIME_ERROR', self.error_details))
            self.process_pool.delete_process_record(self.process_record)
            return False
        
        return True
        
    def line_event_loop(self, reader, writer):
        """Dummy event loop for testing interactive tasks."""
        while True:
            line = reader.readline()
            if line == '':
                return True
            
            argv = line.split()
            
            if argv[0] == 'exit':
                return True
            elif argv[0] == 'echo':
                print argv[1:]
            elif argv[0] == 'filename':
                print argv[1]
            else:
                print 'Unrecognised command:', argv
        
    def open_ref(self, ref, accept_string=False, make_sweetheart=False):
        """Fetches a reference if it is available, and returns a filename for reading it.
        Options to do with eagerness, streaming, etc.
        If reference is unavailable, raises a ReferenceUnavailableException."""
        ref = self.task_record.retrieve_ref(ref)
        if not accept_string:   
            ctx = retrieve_filename_for_ref(ref, self.task_record, return_ctx=True)
        else:
            ctx = retrieve_file_or_string_for_ref(ref, self.task_record)
        if ctx.completed_ref is not None:
            if make_sweetheart:
                ctx.completed_ref = SW2_SweetheartReference.from_concrete(ctx.completed_ref, get_own_netloc())
            self.task_record.publish_ref(ctx.completed_ref)
        return ctx.to_safe_dict()
        
    def publish_fetched_ref(self, fetch):
        completed_ref = fetch.get_completed_ref()
        if completed_ref is None:
            ciel.log("Cancelling async fetch %s (chunk %d)" % (fetch.ref.id, fetch.chunk_size), "EXEC", logging.DEBUG)
        else:
            if fetch.make_sweetheart:
                completed_ref = SW2_SweetheartReference.from_concrete(completed_ref, get_own_netloc())
            self.task_record.publish_ref(completed_ref)
        
    # Setting fd_socket_name implies you can accept a sendmsg'd FD.
    def open_ref_async(self, ref, chunk_size, sole_consumer=False, make_sweetheart=False, must_block=False, fd_socket_name=None):
        if not sendmsg_enabled:
            fd_socket_name = None
            ciel.log("Not using FDs directly: module 'sendmsg' not available", "EXEC", logging.DEBUG)
        real_ref = self.task_record.retrieve_ref(ref)

        new_fetch = OngoingFetch(real_ref, chunk_size, self.task_record, sole_consumer, make_sweetheart, must_block, can_accept_fd=(fd_socket_name is not None))
        ret = {"sending_fd": False}
        ret_fd = None
        if fd_socket_name is not None:
            fd, fd_blocking = new_fetch.get_fd()
            if fd is not None:
                ret["sending_fd"] = True
                ret["blocking"] = fd_blocking
                ret_fd = fd
        if not ret["sending_fd"]:
            filename, file_blocking = new_fetch.get_filename()
            ret["filename"] = filename
            ret["blocking"] = file_blocking
        if not new_fetch.done:
            self.context_manager.add_context(new_fetch)
            self.ongoing_fetches.append(new_fetch)
        else:
            self.publish_fetched_ref(new_fetch)
        # Definitions here: "done" means we're already certain that the producer has completed successfully.
        # "blocking" means that EOF, as and when it arrives, means what it says. i.e. it's a regular file and done, or a pipe-like thing.
        ret.update({"done": new_fetch.done, "size": new_fetch.bytes})
        ciel.log("Async fetch %s (chunk %d): initial status %d bytes, done=%s, blocking=%s, sending_fd=%s" % (real_ref, chunk_size, ret["size"], ret["done"], ret["blocking"], ret["sending_fd"]), "EXEC", logging.DEBUG)

        # XXX: adding this because the OngoingFetch isn't publishing the sweetheart correctly.        
        if make_sweetheart:
            self.task_record.publish_ref(SW2_SweetheartReference(ref.id, get_own_netloc()))

        if new_fetch.done:
            if not new_fetch.success:
                ciel.log("Async fetch %s failed early" % ref, "EXEC", logging.WARNING)
                ret["error"] = "EFAILED"
        return (ret, ret_fd)
    
    def close_async_file(self, id, chunk_size):
        for fetch in self.ongoing_fetches:
            if fetch.ref.id == id and fetch.chunk_size == chunk_size:
                self.publish_fetched_ref(fetch)
                self.context_manager.remove_context(fetch)
                self.ongoing_fetches.remove(fetch)
                return
        #ciel.log("Ignored cancel for async fetch %s (chunk %d): not in progress" % (id, chunk_size), "EXEC", logging.WARNING)

    def wait_async_file(self, id, eof=None, bytes=None):
        the_fetch = None
        for fetch in self.ongoing_fetches:
            if fetch.ref.id == id:
                the_fetch = fetch
                break
        if the_fetch is None:
            ciel.log("Failed to wait for async-fetch %s: not an active transfer" % id, "EXEC", logging.WARNING)
            return {"success": False}
        if eof is not None:
            ciel.log("Waiting for fetch %s to complete" % id, "EXEC", logging.DEBUG)
            the_fetch.wait_eof()
        else:
            ciel.log("Waiting for fetch %s length to exceed %d bytes" % (id, bytes), "EXEC", logging.DEBUG)
            the_fetch.wait_bytes(bytes)
        if the_fetch.done and not the_fetch.success:
            ciel.log("Wait %s complete: transfer has failed" % id, "EXEC", logging.WARNING)
            return {"success": False}
        else:
            ret = {"size": int(the_fetch.bytes), "done": the_fetch.done, "success": True}
            ciel.log("Wait %s complete: new length=%d, EOF=%s" % (id, ret["size"], ret["done"]), "EXEC", logging.DEBUG)
            return ret
        
    def spawn(self, request_args):
        """Spawns a child task. Arguments define a task_private structure. Returns a list
        of future references."""
        
        # Args dict arrives from sw with unicode keys :(
        str_args = dict([(str(k), v) for (k, v) in request_args.items()])
        
        if "small_task" not in str_args:
            str_args['small_task'] = False
        
        return spawn_task_helper(self.task_record, **str_args)
    
    def tail_spawn(self, request_args):
        
        if request_args.get("is_fixed", False):
            request_args["process_record_id"] = self.process_record.id
        request_args["delegated_outputs"] = self.task_descriptor["expected_outputs"]
        self.spawn(request_args)
    
    def allocate_output(self, prefix=""):
        new_output_name = self.task_record.create_published_output_name(prefix)
        self.expected_outputs.append(new_output_name)
        return {"index": len(self.expected_outputs) - 1}
    
    def publish_string(self, index, str):
        """Defines a reference with the given string contents."""
        ref = ref_from_safe_string(str, self.expected_outputs[index])
        self.task_record.publish_ref(ref)
        return {"ref": ref}

    def open_output(self, index, may_pipe=False, may_stream=False, make_local_sweetheart=False, can_smart_subscribe=False, fd_socket_name=None):
        if may_pipe and not may_stream:
            raise Exception("Insane parameters: may_stream=False and may_pipe=True may well lead to deadlock")
        if index in self.ongoing_outputs:
            raise Exception("Tried to open output %d which was already open" % index)
        if not sendmsg_enabled:
            ciel.log("Not using FDs directly: module 'sendmsg' not available", "EXEC", logging.DEBUG)
            fd_socket_name = None
        output_name = self.expected_outputs[index]
        can_accept_fd = (fd_socket_name is not None)
        output_ctx = OngoingOutput(output_name, index, can_smart_subscribe, may_pipe, make_local_sweetheart, can_accept_fd, self)
        self.ongoing_outputs[index] = output_ctx
        self.context_manager.add_context(output_ctx)
        if may_stream:
            ref = output_ctx.get_stream_ref()
            self.task_record.prepublish_refs([ref])
        x, is_fd = output_ctx.get_filename_or_fd()
        if is_fd:
            return ({"sending_fd": True}, x)
        else:
            return ({"sending_fd": False, "filename": x}, None)

    def stop_output(self, index):
        self.context_manager.remove_context(self.ongoing_outputs[index])
        del self.ongoing_outputs[index]

    def close_output(self, index, size=None):
        output = self.ongoing_outputs[index]
        if size is None:
            size = output.get_size()
        output.size_update(size)
        self.stop_output(index)
        ret_ref = output.get_completed_ref()
        self.task_record.publish_ref(ret_ref)
        return {"ref": ret_ref}

    def log(self, message):
        t = datetime.datetime.now()
        timestamp = time.mktime(t.timetuple()) + t.microsecond / 1e6
        self.worker.master_proxy.log(self.task_descriptor["job"], self.task_descriptor["task_id"], timestamp, message)

    def rollback_output(self, index):
        self.ongoing_outputs[index].rollback()
        self.stop_output(index)

    def output_size_update(self, index, size):
        self.ongoing_outputs[index].size_update(size)

    def _subscribe_output(self, index, chunk_size):
        message = ("subscribe", {"index": index, "chunk_size": chunk_size})
        with self.transmit_lock:
            write_framed_json(message, self.writer)

    def _unsubscribe_output(self, index):
        message = ("unsubscribe", {"index": index})
        with self.transmit_lock:
            write_framed_json(message, self.writer)
           
    def json_event_loop(self, reader, writer):
        while True:

            try:
                (method, args) = read_framed_json(reader)
            except:
                ciel.log('Error reading in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
                
            #ciel.log('Method is %s' % repr(method), 'PROC', logging.INFO)
            response = None
            response_fd = None
            
            try:
                if method == 'open_ref':
                    
                    if "ref" not in args:
                        ciel.log('Missing required argument key: ref', 'PROC', logging.ERROR, False)
                        return PROC_ERROR
                    
                    try:
                        response = self.open_ref(**args)
                    except ReferenceUnavailableException:
                        response = {'error' : 'EWOULDBLOCK'}
                        
                elif method == "open_ref_async":
                    
                    if "ref" not in args or "chunk_size" not in args:
                        ciel.log("Missing required argument key: open_ref_async needs both 'ref' and 'chunk_size'", "PROC", logging.ERROR, False)
                        return PROC_ERROR
            
                    try:
                        response, response_fd = self.open_ref_async(**args)
                    except ReferenceUnavailableException:
                        response = {"error": "EWOULDBLOCK"}
                        
                elif method == "wait_stream":
                    response = self.wait_async_file(**args)
                    
                elif method == "close_stream":
                    self.close_async_file(args["id"], args["chunk_size"])
                    
                elif method == 'spawn':
                    
                    response = self.spawn(args)
                                        
                elif method == 'tail_spawn':
                    
                    response = self.tail_spawn(args)
                    
                elif method == 'allocate_output':
                    
                    response = self.allocate_output(**args)
                    
                elif method == 'publish_string':
                    
                    response = self.publish_string(**args)

                elif method == 'log':
                    # No response.
                    self.log(**args)

                elif method == 'open_output':
                    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    
                    response, response_fd = self.open_output(**args)
                        
                elif method == 'close_output':
    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    response = self.close_output(**args)
                    
                elif method == 'rollback_output':
    
                    try:
                        index = int(args['index'])
                        if index < 0 or index > len(self.expected_outputs):
                            ciel.log('Invalid argument value: i (index) out of bounds [0, %s)' % self.expected_outputs, 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                    except KeyError:
                        if len(self.task_descriptor['expected_outputs']) == 1:
                            args["index"] = 0
                        else:
                            ciel.log('Missing argument key: i (index), and >1 expected output so could not infer index', 'PROC', logging.ERROR, False)
                            return PROC_ERROR
                        
                    response = {'ref' : self.rollback_output(**args)}
                    
                elif method == "advert":
                    self.output_size_update(**args)
    
                elif method == "package_lookup":
                    response = {"value": package_lookup(self.task_record, self.block_store, args["key"])}
    
                elif method == 'error':
                    ciel.log('Task reported error: %s' % args["report"], 'PROC', logging.ERROR, False)
                    raise TaskFailedError(args["report"])
    
                elif method == 'exit':
                    
                    if args["keep_process"] == "must_keep":
                        return PROC_MUST_KEEP
                    elif args["keep_process"] == "may_keep":
                        self.soft_cache_keys = args.get("soft_cache_keys", [])
                        return PROC_MAY_KEEP
                    elif args["keep_process"] == "no":
                        return PROC_EXITED
                    else:
                        ciel.log("Bad exit status from task: %s" % args, "PROC", logging.ERROR)
                
                else:
                    ciel.log('Invalid method: %s' % method, 'PROC', logging.WARN, False)
                    return PROC_ERROR

            except MissingInputException, mie:
                ciel.log("Task died due to missing input", 'PROC', logging.WARN)
                raise

            except TaskFailedError:
                raise

            except:
                ciel.log('Error during method handling in JSON event loop', 'PROC', logging.ERROR, True)
                return PROC_ERROR
        
            try:
                if response is not None:
                    with self.transmit_lock:
                        write_framed_json((method, response), writer)
                if response_fd is not None:
                    socket_name = args["fd_socket_name"]
                    sock = socket.socket(socket.AF_UNIX)
                    sock.connect(socket_name)
                    sendmsg.sendmsg(fd=sock.fileno(), data="FD", ancillary=(socket.SOL_SOCKET, sendmsg.SCM_RIGHTS, struct.pack("i", response_fd)))
                    os.close(response_fd)
                    sock.close()
            except:
                ciel.log('Error writing response in JSON event loop', 'PROC', logging.WARN, True)
                return PROC_ERROR
        
        return True
    