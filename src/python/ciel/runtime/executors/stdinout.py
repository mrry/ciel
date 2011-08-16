# Copyright (c) 2010--2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#                          Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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
from ciel.public.references import SWRealReference
import stat
import os
import subprocess
import ciel
import logging
import shutil
from subprocess import PIPE
import errno
from ciel.runtime.executors.simple import ProcessRunningExecutor
from ciel.runtime.exceptions import BlameUserException
from ciel.runtime.fetcher import retrieve_filename_for_ref
from ciel.runtime.executors import list_with

class SWStdinoutExecutor(ProcessRunningExecutor):
    
    handler_name = "stdinout"

    def __init__(self, worker):
        ProcessRunningExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        ProcessRunningExecutor.check_args_valid(args, n_outputs)
        if n_outputs != 1:
            raise BlameUserException("Stdinout executor must have one output")
        if "command_line" not in args:
            raise BlameUserException('Incorrect arguments to the stdinout executor: %s' % repr(args))

    def start_process(self, input_files, output_files):

        command_line = self.args["command_line"]

        for i, arg in enumerate(command_line):
            if isinstance(arg, SWRealReference):
                # Command line argument has been passed in as a reference.
                command_line[i] = retrieve_filename_for_ref(arg, self.task_record, False)
                if i == 0:
                    # First argument must be executable.
                    os.chmod(command_line[0], stat.S_IRWXU)
        
        ciel.log.error("Executing stdinout with: %s" % " ".join(map(str, command_line)), 'EXEC', logging.DEBUG)

        with open(output_files[0], "w") as temp_output_fp:
            # This hopefully avoids the race condition in subprocess.Popen()
            return subprocess.Popen(map(str, command_line), stdin=PIPE, stdout=temp_output_fp, close_fds=True)

    def await_process(self, input_files, output_files):

        with list_with([open(filename, 'r') for filename in input_files]) as fileobjs:
            for fileobj in fileobjs:
                try:
                    shutil.copyfileobj(fileobj, self.proc.stdin)
                except IOError, e:
                    if e.errno == errno.EPIPE:
                        ciel.log.error('Abandoning cat due to EPIPE', 'EXEC', logging.WARNING)
                        break
                    else:
                        raise

        self.proc.stdin.close()
        rc = self.proc.wait()
        return rc
