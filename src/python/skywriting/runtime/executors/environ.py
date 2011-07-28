# Copyright (c) 2010--11 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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
from shared.references import SWRealReference
import os
import stat
import ciel
import tempfile
import subprocess
from skywriting.runtime.executors.simple import ProcessRunningExecutor
from skywriting.runtime.exceptions import BlameUserException
from skywriting.runtime.fetcher import retrieve_filename_for_ref
import logging

class EnvironmentExecutor(ProcessRunningExecutor):

    handler_name = "env"

    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        ProcessRunningExecutor.check_args_valid(args, n_outputs)
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the env executor: %s' % repr(args))

    def start_process(self, input_files, output_files):

        command_line = self.args["command_line"]

        for i, arg in enumerate(command_line):
            if isinstance(arg, SWRealReference):
                # Command line argument has been passed in as a reference.
                command_line[i] = retrieve_filename_for_ref(arg, self.task_record, False)
                if i == 0:
                    # First argument must be executable.
                    os.chmod(command_line[0], stat.S_IRWXU)

        try:
            env = self.args['env']
        except KeyError:
            env = {}

        ciel.log.error("Executing environ with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.DEBUG)

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
        
        environment.update(os.environ)
        environment.update(env)
            
        proc = subprocess.Popen(map(str, command_line), env=environment, close_fds=True)

        #_ = proc.stdout.read(1)
        #print 'Got byte back from Executor'

        return proc
