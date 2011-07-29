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
from skywriting.runtime.executors.proc import ProcExecutor
import os
from skywriting.runtime.exceptions import BlameUserException
from skywriting.runtime.executors import add_package_dep, test_program
import ciel
import logging

class SkyPyExecutor(ProcExecutor):

    handler_name = "skypy"
    skypybase = os.getenv("CIEL_SKYPY_BASE", None)
    process_cache = set()
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, pyfile_ref=None, coro_ref=None, entry_point=None, entry_args=None, export_json=False, run_fixed=False, is_tail_spawn=False, n_extra_outputs=0, **kwargs):

        if pyfile_ref is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All SkyPy invocations must specify a .py file reference as 'pyfile_ref' or else reference a fixed process")
        if coro_ref is None and (entry_point is None or entry_args is None) and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All SkyPy invocations must specify either coro_ref or entry_point and entry_args, or else reference a fixed process")

        if not is_tail_spawn:
            ret_output = "%s:retval" % task_descriptor["task_id"]
            task_descriptor["expected_outputs"].append(ret_output)
            task_descriptor["task_private"]["ret_output"] = 0
            extra_outputs = ["%s:out:%d" % (task_descriptor["task_id"], i) for i in range(n_extra_outputs)]
            task_descriptor["expected_outputs"].extend(extra_outputs)
            task_descriptor["task_private"]["extra_outputs"] = range(1, n_extra_outputs + 1)
        
        if coro_ref is not None:
            task_descriptor["task_private"]["coro_ref"] = coro_ref
            task_descriptor["dependencies"].append(coro_ref)
        else:
            task_descriptor["task_private"]["entry_point"] = entry_point
            task_descriptor["task_private"]["entry_args"] = entry_args
        if not is_tail_spawn:
            task_descriptor["task_private"]["export_json"] = export_json
            task_descriptor["task_private"]["run_fixed"] = run_fixed
        task_descriptor["task_private"]["is_continuation"] = is_tail_spawn
        if pyfile_ref is not None:
            task_descriptor["task_private"]["py_ref"] = pyfile_ref
            task_descriptor["dependencies"].append(pyfile_ref)
        add_package_dep(parent_task_record.package_ref, task_descriptor)

        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record,  
                                                    is_tail_spawn=is_tail_spawn, **kwargs)

    def get_command(self):
        return ["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py")]
        
    def get_env(self):
        return {"PYTHONPATH": SkyPyExecutor.skypybase + ":" + os.environ["PYTHONPATH"]}

    @staticmethod
    def can_run():
        if SkyPyExecutor.skypybase is None:
            ciel.log.error("Can't run SkyPy: CIEL_SKYPY_BASE not in environment", "SKYPY", logging.WARNING)
            return False
        else:
            return test_program(["pypy", os.path.join(SkyPyExecutor.skypybase, "stub.py"), "--version"], "PyPy")
