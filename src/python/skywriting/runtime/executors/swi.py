# Copyright (c) 2010-2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#                         Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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

class SkywritingExecutor(ProcExecutor):

    handler_name = "swi"
    process_cache = set()

    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, sw_file_ref=None, start_env=None, start_args=None, cont_ref=None, n_extra_outputs=0, is_tail_spawn=False, **kwargs):

        if sw_file_ref is None and cont_ref is None:
            raise BlameUserException("Skywriting tasks must specify either a continuation object or a .sw file")
        if n_extra_outputs > 0:
            raise BlameUserException("Skywriting can't deal with extra outputs")

        if not is_tail_spawn:
            task_descriptor["expected_outputs"] = ["%s:retval" % task_descriptor["task_id"]]
            task_descriptor["task_private"]["ret_output"] = 0

        if cont_ref is not None:
            task_descriptor["task_private"]["cont"] = cont_ref
            task_descriptor["dependencies"].append(cont_ref)
        else:
            # External call: SW file should be started from the beginning.
            task_descriptor["task_private"]["swfile_ref"] = sw_file_ref
            task_descriptor["dependencies"].append(sw_file_ref)
            task_descriptor["task_private"]["start_env"] = start_env
            task_descriptor["task_private"]["start_args"] = start_args
        add_package_dep(parent_task_record.package_ref, task_descriptor)
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, 
                                                  is_tail_spawn=is_tail_spawn, is_fixed=False, **kwargs)

    def get_command(self):
        return ["skywriting"]

    @staticmethod
    def can_run():
        return test_program(["skywriting", "--version"], "Skywriting")
