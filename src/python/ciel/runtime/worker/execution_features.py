# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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
from ciel.runtime.executors.stdinout import SWStdinoutExecutor
from ciel.runtime.executors.dotnet import DotNetExecutor
from ciel.runtime.executors.environ import EnvironmentExecutor
from ciel.runtime.executors.cso import CExecutor
from ciel.runtime.executors.grab import GrabURLExecutor
from ciel.runtime.executors.sync import SyncExecutor
from ciel.runtime.executors.init import InitExecutor
from ciel.runtime.executors.proc import ProcExecutor
from ciel.runtime.executors.ocaml import OCamlExecutor
from ciel.runtime.executors.haskell import HaskellExecutor
from ciel.runtime.executors.java import JavaExecutor
from ciel.runtime.executors.java2 import Java2Executor
import ciel
import logging
import pkg_resources

class ExecutionFeatures:
    
    def __init__(self):

        self.executors = dict([(x.handler_name, x) for x in [SWStdinoutExecutor, 
                                                             EnvironmentExecutor, DotNetExecutor, 
                                                             CExecutor, GrabURLExecutor, SyncExecutor, InitExecutor,
                                                             OCamlExecutor, HaskellExecutor,
                                                             ProcExecutor, JavaExecutor, Java2Executor]])

        for entrypoint in pkg_resources.iter_entry_points(group="ciel.executor.plugin"):
            classes_function = entrypoint.load()
            plugin_classes = classes_function()
            for plugin_class in plugin_classes:
                ciel.log("Found plugin for %s executor" % plugin_class.handler_name, 'EXEC', logging.INFO)
                self.executors[plugin_class.handler_name] = plugin_class

        self.runnable_executors = dict([(x, self.executors[x]) for x in self.check_executors()])
        # TODO: Implement a class method for this.
        cacheable_executor_names = set(['swi', 'skypy', 'java2'])
        self.process_cacheing_executors = [self.runnable_executors[x] 
                                           for x in cacheable_executor_names & set(self.runnable_executors.keys())]

    def all_features(self):
        return self.executors.keys()

    def check_executors(self):
        ciel.log.error("Checking executors:", "EXEC", logging.INFO)
        retval = []
        for (name, executor) in self.executors.items():
            if executor.can_run():
                ciel.log.error("Executor '%s' can run" % name, "EXEC", logging.INFO)
                retval.append(name)
            else:
                ciel.log.error("Executor '%s' CANNOT run" % name, "EXEC", logging.INFO)
        return retval
    
    def can_run(self, name):
        return name in self.runnable_executors

    def get_executor(self, name, worker):
        try:
            return self.runnable_executors[name](worker)
        except KeyError:
            raise Exception("Executor %s not installed" % name)

    def get_executor_class(self, name):
        try:
            return self.executors[name]
        except KeyError:
            raise Exception("Executor %s not installed" % name)
