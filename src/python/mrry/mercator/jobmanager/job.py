'''
Created on 8 Feb 2010

@author: dgm36
'''
from uuid import uuid4
from mrry.mercator.jobmanager.inputs import JobArguments, JobLiteralInput,\
    MultipleSourceJobInput, HttpJobInput, LocalFileJobInput, JobOutputs
import simplejson
import threading
import struct
import subprocess
import os

class Job:
    
    def __init__(self):
        self.id = str(uuid4())

output_dir = '/tmp/'

class CloudScriptJob:
    
    def __init__(self, master_task_id, master_task_attempt_id, executor, args, arg_representations, outputs):
        self.master_task_id = master_task_id
        self.id = master_task_attempt_id
        self.executor = executor
        self.args = args
        self.arg_representations = arg_representations
        self.outputs = outputs
    
        self.fetched_arg_filenames = {}
        
        self._lock = threading.Lock()

    def set_input_filename(self, input, filename):
        with self._lock:
            self.fetched_arg_filenames[input] = filename
        
    def rewrite_single_arg(self, arg):
        if arg[0] == 'list':
            return ('list', self.rewrite_args_list(arg[1]))
        elif arg[0] == 'dict':
            return ('dict', self.rewrite_args_dict(arg[1]))
        elif arg[0] == 'datum':
            return ('file', self.fetched_arg_filenames[arg[1]])
        else:
            return arg
    
    def rewrite_args_list(self, args_list):
        ret = []
        for arg in args_list:
            ret.append(self.rewrite_single_arg(arg))
        return ret
            
    def rewrite_args_dict(self, args_dict):
        ret = {}
        for (name, arg) in args_dict.items():
            ret[name] = self.rewrite_single_arg(arg)
        return ret

    def is_runnable(self):
        with self._lock:
            return len(self.fetched_arg_filenames) == len(self.arg_representations)
        
    def allocate_output_filenames(self):
        output_path = os.path.join(output_dir, str(self.id))
        os.mkdir(output_path)
        
        return [('file', os.path.join(output_path, str(x))) for x in self.outputs]
                 
    def run(self, bus):

        real_args = self.rewrite_args_dict(self.args)
        real_outputs = self.allocate_output_filenames()
        
        process_args = {}
        process_args['inputs'] = real_args
        process_args['outputs'] = real_outputs
        
        stdin_file = open("%s.in" % (self.id, ), "r")
        stdout_file = open("%s.out" % (self.id, ), "w")
        stderr_file = open("%s.err" % (self.id, ), "w")
        
        self.proc = subprocess.Popen(args=[], close_fds=True, stdin=stdin_file, stdout=stdout_file, stderr=stderr_file)
        bus.publish('update_status', self.id, "RUNNING")
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))

class WorkflowJob(Job):
    
    def __init__(self, executor_process, inputs=[], outputs=[]):
        Job.__init__(self)
        self.executor_process = executor_process
        self._lock = threading.Lock()
        self.args = JobArguments(self, inputs)
        self.outputs = JobOutputs(self, outputs)
        
    def set_input_filename(self, input, filename):
        with self._lock:
            self.args.pending_inputs.remove(input.id)
        input.filename = filename
        
    def is_runnable(self):
        with self._lock:
            return len(self.args.pending_inputs) == 0
        
    def run(self, bus):
        
        # Create arguments file.
        arguments = {}
        arguments["inputs"] = self.args.get_input_structure()
        arguments["outputs"] = self.outputs.get_output_structure()
        with open("%s.in" % (self.id, ), "w") as args_file:
            simplejson.dump(arguments, args_file)
        
        stdin_file = open("%s.in" % (self.id, ), "r")
        stdout_file = open("%s.out" % (self.id, ), "w")
        stderr_file = open("%s.err" % (self.id, ), "w")
        
        self.proc = subprocess.Popen(args=[], close_fds=True, stdin=stdin_file, stdout=stdout_file, stderr=stderr_file)
        bus.publish('update_status', self.id, "RUNNING")
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))
        
class SubprocessJob(Job):

    def __init__(self, args):
        Job.__init__(self)
        self.args = args
        self.proc = None

    def kill(self):
        if self.proc is not None:
            self.proc.kill()

    def run(self, bus):
        stdout_file = open("%s.out" % (self.id, ), "w")
        stderr_file = open("%s.err" % (self.id, ), "w")
        self.proc = subprocess.Popen(args=self.args, close_fds=True, stdin=subprocess.PIPE, stdout=stdout_file, stderr=stderr_file)
        bus.publish('update_status', self.id, "RUNNING")
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))

class CommunicableSubprocessJob(SubprocessJob):
    
    def __init__(self, args):
        SubprocessJob.__init__(self, args)
        self.is_running = False
        self.lock = threading.Lock()
        
    def send_message(self, message):
        with self.lock:
            if self.is_running:
                self.proc.stdin.write(struct.pack("I", len(message)))
                self.proc.stdin.write(message)
                self.proc.stdin.flush()
            else:
                raise
        
    def run(self, bus):
        stderr_file = open("%s.err" % (self.id, ), "w")
        self.proc = subprocess.Popen(args=self.args, close_fds=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file)
        
        with self.lock:
            self.is_running = True
        
        bus.publish('update_status', self.id, "RUNNING")
        sizeof_uint = struct.calcsize("I")
            
        while True:
            length_str = self.proc.stdout.read(sizeof_uint)
            length, = struct.unpack("I", length_str)
                
            # The final message from the process will be zero-length.
            if length == 0:
                break
                
            bus.log("Length is: %s" % (length, ))    
            
            
            message = self.proc.stdout.read(length)
            bus.publish('update_status', self.id, ("RUNNING", message))
            
        with self.lock:
            self.is_running = False
                
        # Finally send a zero-length message to the process to acknowledge that
        # it is terminated. At this point, we know that we will not try to send
        # any more messages to the process.
        self.proc.stdin.write(struct.pack("I", 0))
        self.proc.stdin.flush()
            
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))

def build_pyecho_job(details):
    return CommunicableSubprocessJob(['python', '/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/python/mrry/mercator/tasks/examples/echo/main.py'])

def build_pysleep_job(details):
    return CommunicableSubprocessJob(['python', '/local/scratch/dgm36/eclipse/workspace/mercator.hg/src/python/mrry/mercator/tasks/examples/sleep/main.py'])

def build_dict_cat_job(details):
    return SubprocessJob(['cat', '/usr/share/dict/words'])

def build_dict_grep_job(details):
    return SubprocessJob(['grep', str(details["search_pattern"]), '/usr/share/dict/words'])

def build_sleep_job(details):
    return SubprocessJob(['sleep', str(details["duration"])])

job_builders = { "pyecho" : build_pyecho_job,
                 "pysleep" : build_pysleep_job,
                 "dict_cat" : build_dict_cat_job, 
                 "dict_grep" : build_dict_grep_job,
                 "sleep" : build_sleep_job }

def build_job(details):
    job_type = details["job_type"]
    return job_builders[job_type](details)

if __name__ == '__main__':
    
    inputs = [JobLiteralInput(None, "/usr/bin/cat").to_external_format(),
              MultipleSourceJobInput(None, "0123", [HttpJobInput(None, "0123", "http://www.cl.cam.ac.uk/~dgm36/test_input.txt"),
                                                    LocalFileJobInput(None, "0123", "heavenly.cl.cam.ac.uk", "/local/scratch/dgm36/test_input.txt")]).to_external_format()]
    
    outputs = [{"id": "01223"}]

    wj = WorkflowJob("stdinout_workflow", inputs, outputs)
    
    print wj.args.get_input_structure()
    print wj.outputs.get_output_structure()
