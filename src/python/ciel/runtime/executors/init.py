# Copyright (c) 2010-11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
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
from ciel.runtime.executors.base import BaseExecutor
import pickle
from ciel.runtime.exceptions import BlameUserException
from ciel.runtime.executors import spawn_task_helper
from ciel.public.references import SWRealReference

# XXX: Passing ref_of_string to get round a circular import. Should really move ref_of_string() to
#      a nice utility package.
def build_init_descriptor(handler, args, package_ref, master_uri, ref_of_string):
    task_private_dict = {"package_ref": package_ref, 
                         "start_handler": handler, 
                         "start_args": args
                         } 
    task_private_ref = ref_of_string(pickle.dumps(task_private_dict), master_uri)
    return {"handler": "init", 
            "dependencies": [package_ref, task_private_ref], 
            "task_private": task_private_ref
            }

class InitExecutor(BaseExecutor):

    handler_name = "init"

    def __init__(self, worker):
        BaseExecutor.__init__(self, worker)

    @staticmethod
    def can_run():
        return True

    @classmethod
    def build_task_descriptor(cls, descriptor, parent_task_record, **args):
        raise BlameUserException("Can't spawn init tasks directly; build them from outside the cluster using 'build_init_descriptor'")

    def _run(self, task_private, task_descriptor, task_record):
        
        args_dict = task_private["start_args"]
        # Some versions of simplejson make these ascii keys into unicode objects :(
        args_dict = dict([(str(k), v) for (k, v) in args_dict.items()])
        initial_task_out_obj = spawn_task_helper(task_record,
                                                 task_private["start_handler"], 
                                                 True,
                                                 **args_dict)
        if isinstance(initial_task_out_obj, SWRealReference):
            initial_task_out_refs = [initial_task_out_obj]
        else:
            initial_task_out_refs = list(initial_task_out_obj)
        spawn_task_helper(task_record, "sync", True, delegated_outputs = task_descriptor["expected_outputs"], args = {"inputs": initial_task_out_refs}, n_outputs=1)
