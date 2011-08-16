# Copyright (c) 2010--11 Derek Murray <derek.murray@cl.cam.ac.uk>
#                        Chris Smowton <chris.smowton@cl.cam.ac.uk>
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
import ciel
import logging
from ciel.runtime.object_cache import retrieve_object_for_ref,\
    ref_from_object

class BaseExecutor:
    
    TASK_PRIVATE_ENCODING = 'pickle'
    
    def __init__(self, worker):
        self.worker = worker
        self.block_store = worker.block_store
    
    def run(self, task_descriptor, task_record):
        # XXX: This is braindead, considering that we just stashed task_private
        #      in here during prepare().
        self._run(task_descriptor["task_private"], task_descriptor, task_record)
    
    def abort(self):
        self._abort()    
    
    def cleanup(self):
        pass
        
    @classmethod
    def prepare_task_descriptor_for_execute(cls, task_descriptor, task_record, block_store):
        # Convert task_private from a reference to an object in here.
        try:
            task_descriptor["task_private"] = retrieve_object_for_ref(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING, task_record)
        except:
            ciel.log('Error retrieving task_private reference from task', 'BASE_EXECUTOR', logging.WARN, True)
            raise
        
    @classmethod
    def build_task_descriptor(cls, task_descriptor, parent_task_record):
        # Convert task_private to a reference in here. 
        task_private_id = ("%s:_private" % task_descriptor["task_id"])
        task_private_ref = ref_from_object(task_descriptor["task_private"], BaseExecutor.TASK_PRIVATE_ENCODING, task_private_id)
        parent_task_record.publish_ref(task_private_ref)
        task_descriptor["task_private"] = task_private_ref
        task_descriptor["dependencies"].append(task_private_ref)
