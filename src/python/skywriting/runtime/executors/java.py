# Copyright (c) 2010--11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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
import os
import ciel
import logging
from skywriting.runtime.executors.simple import FilenamesOnStdinExecutor
from skywriting.runtime.executors import test_program
from skywriting.runtime.exceptions import BlameUserException
from skywriting.runtime.fetcher import retrieve_filenames_for_refs


class JavaExecutor(FilenamesOnStdinExecutor):

    handler_name = "java"

    def __init__(self, worker):
        FilenamesOnStdinExecutor.__init__(self, worker)

    @staticmethod
    def can_run():
        cp = os.getenv("CLASSPATH")
        if cp is None:
            ciel.log.error("Can't run Java: no CLASSPATH set", "JAVA", logging.WARNING)
            return False
        else:
            return test_program(["java", "-cp", cp, "uk.co.mrry.mercator.task.JarTaskLoader", "--version"], "Java")

    @classmethod
    def check_args_valid(cls, args, n_outputs):

        FilenamesOnStdinExecutor.check_args_valid(args, n_outputs)
        if "lib" not in args or "class" not in args:
            raise BlameUserException('Incorrect arguments to the java executor: %s' % repr(args))

    def before_execute(self):

        self.jar_refs = self.args["lib"]
        self.class_name = self.args["class"]

        ciel.log.error("Running Java executor for class: %s" % self.class_name, "JAVA", logging.DEBUG)
        ciel.engine.publish("worker_event", "Java: fetching JAR")

        self.jar_filenames = retrieve_filenames_for_refs(self.jar_refs, self.task_record)

    def get_process_args(self):
        cp = os.getenv('CLASSPATH')
        process_args = ["java", "-cp", cp]
        if "trace_io" in self.debug_opts:
            process_args.append("-Dskywriting.trace_io=1")
        process_args.extend(["uk.co.mrry.mercator.task.JarTaskLoader", self.class_name])
        process_args.extend(["file://" + x for x in self.jar_filenames])
        return process_args
