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
from ciel.runtime.executors.proc import ProcExecutor
from ciel.runtime.exceptions import BlameUserException
import hashlib
from ciel.runtime.executors import hash_update_with_structure,\
    add_package_dep, test_program
from ciel.runtime.fetcher import retrieve_filename_for_ref
import ciel
import os
import logging
import pkg_resources
import tempfile
import shutil

REQUIRED_LIBS = ['ciel-0.1.jar', 'gson-1.7.1.jar']

class Java2Executor(ProcExecutor):
    
    handler_name = "java2"
    process_cache = set()

    classpath = None
    
    def __init__(self, worker):
        ProcExecutor.__init__(self, worker)

    @classmethod
    def check_args_valid(cls, args, n_outputs):
        if "class_name" not in args and "object_ref" not in args:
            raise BlameUserException("All Java2 invocations must specify either a class_name or an object_ref")
        if "jar_lib" not in args:
            raise BlameUserException("All Java2 invocations must specify a jar_lib")
            
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record, jar_lib=None, args=None, class_name=None, object_ref=None, n_outputs=1, is_tail_spawn=False, **kwargs):
        # More good stuff goes here.
        if jar_lib is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All Java2 invocations must either specify jar libs or an existing process ID")
        if class_name is None and object_ref is None and kwargs.get("process_record_id", None) is None:
            raise BlameUserException("All Java2 invocations must specify either a class_name or an object_ref, or else give a process ID")
        
        if jar_lib is not None:
            task_descriptor["task_private"]["jar_lib"] = jar_lib
            for jar_ref in jar_lib:
                task_descriptor["dependencies"].append(jar_ref)

        if not is_tail_spawn:
            sha = hashlib.sha1()
            hash_update_with_structure(sha, [args, n_outputs])
            hash_update_with_structure(sha, class_name)
            hash_update_with_structure(sha, object_ref)
            hash_update_with_structure(sha, jar_lib)
            name_prefix = "java2:%s:" % (sha.hexdigest())
            task_descriptor["expected_outputs"] = ["%s%d" % (name_prefix, i) for i in range(n_outputs)]            
        
        if class_name is not None:
            task_descriptor["task_private"]["class_name"] = class_name
        if object_ref is not None:
            task_descriptor["task_private"]["object_ref"] = object_ref
            task_descriptor["dependencies"].append(object_ref)
        if args is not None:
            task_descriptor["task_private"]["args"] = args
        add_package_dep(parent_task_record.package_ref, task_descriptor)
        
        return ProcExecutor.build_task_descriptor(task_descriptor, parent_task_record, n_extra_outputs=0, is_tail_spawn=is_tail_spawn, accept_ref_list_for_single=True, **kwargs)
        
    def get_command(self):
        jar_filenames = []
        for ref in self.task_private['jar_lib']:
            obj_store_filename = retrieve_filename_for_ref(ref, self.task_record, False)
            with open(obj_store_filename, 'r') as f:
                with tempfile.NamedTemporaryFile(suffix='.jar', delete=False) as temp_f:
                    shutil.copyfileobj(f, temp_f)
                    jar_filenames.append(temp_f.name)
        return ["java", "-Xmx2048M", "-cp", str(':'.join(jar_filenames)), "com.asgow.ciel.executor.Java2Executor"]

    @staticmethod
    def can_run():
        return test_program(["java", "-version"], "Java")
