'''
Created on 8 Feb 2010

@author: dgm36
'''
from uuid import uuid4
import struct
import subprocess

class Job:
    
    def __init__(self):
        self.id = str(uuid4())


class SubprocessJob(Job):

    def __init__(self, args):
        Job.__init__(self)
        self.args = args

    # TODO: manage states of process, communicate with process (raise exception)

    def kill(self):
        self.proc.kill()

    def run(self, bus):
        stdout_file = open("%s.out" % (self.id, ), "w")
        stderr_file = open("%s.err" % (self.id, ), "w")
        self.proc = subprocess.Popen(args=self.args, close_fds=True, stdin=subprocess.PIPE, stdout=stdout_file, stderr=stderr_file)
        self.bus.publish('update_status', self.id, "RUNNING")
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))

class CommunicableSubprocessJob(SubprocessJob):
    
    def __init__(self, args):
        SubprocessJob.__init__(self, args)
        
    def run(self, bus):
        stderr_file = open("%s.err" % (self.id, ), "w")
        self.proc = subprocess.Popen(args=self.args, close_fds=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr_file)
        
        bus.publish('update_status', self.id, "RUNNING")
        sizeof_uint = struct.calcsize("I")
            
        while True:
            length_str = self.proc.stdout.read(sizeof_uint)
            length = struct.unpack("I", length_str)
                
            # The final message from the process will be zero-length.
            if length == 0:
                break
                
            message = self.proc.stdout.read(length)
            bus.publish('update_status', self.id, ("RUNNING", message))
                
        # Finally send a zero-length message to the process to acknowledge that
        # it is terminated. At this point, we know that we will not try to send
        # any more messages to the process.
        self.proc.stdin.write(struct.pack("I", 0))
        self.proc.stdin.flush()
            
        rc = self.proc.wait()
        bus.publish('update_status', self.id, ("TERMINATED", rc))
            
            
        

def build_dict_cat_job(details):
    return SubprocessJob(['cat', '/usr/share/dict/words'])

def build_dict_grep_job(details):
    return SubprocessJob(['grep', str(details["search_pattern"]), '/usr/share/dict/words'])

def build_sleep_job(details):
    return SubprocessJob(['sleep', str(details["duration"])])

job_builders = { "dict_cat" : build_dict_cat_job, 
                 "dict_grep" : build_dict_grep_job,
                 "sleep" : build_sleep_job }

def build_job(details):
    job_type = details["job_type"]
    return job_builders[job_type](details)