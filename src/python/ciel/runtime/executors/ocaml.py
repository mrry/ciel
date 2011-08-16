# Copyright (c) 2011 Anil Madhavapeddy <Anil.Madhavapeddy@cl.cam.ac.uk>
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
from ciel.runtime.executors.proc import ProcExecutor
from ciel.runtime.exceptions import BlameUserException
import hashlib
from ciel.runtime.executors import hash_update_with_structure,\
    test_program

class OCamlExecutor(ProcExecutor):
    
    handler_name = "ocaml"
    process_cache = set()
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "binary" not in args:
            raise BlameUserException("All OCaml invocations must specify a binary")
            
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, binary, fn_ref=None, args=None, n_outputs=1, is_tail_spawn=False, **kwargs):
        if binary is None:
            raise BlameUserException("All OCaml invocations must specify a binary")
        
        task_descriptor["task_private"]["binary"] = binary
        if fn_ref is not None:
            task_descriptor["task_private"]["fn_ref"] = fn_ref
            task_descriptor["dependencies"].append(fn_ref)

        if not is_tail_spawn:
            sha = hashlib.sha1()
            hash_update_with_structure(sha, [args, n_outputs])
            hash_update_with_structure(sha, binary)
            hash_update_with_structure(sha, fn_ref)
            name_prefix = "ocaml:%s:" % (sha.hexdigest())
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]            
        
        if args is not None:
            task_descriptor["task_private"]["args"] = args
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, n_extra_outputs=0, is_tail_spawn=is_tail_spawn, is_fixed=False, accept_ref_list_for_single=True, **kwargs)
        
    def get_command(self):
        cmd = self.task_descriptor["task_private"]["binary"]
        assert(cmd)
        return cmd.split(" ")

    @staticmethod
    def can_run():
        return test_program(["ocamlc", "-where"], "OCaml")
