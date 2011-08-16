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
from __future__ import with_statement

from ciel.public.references import \
    SWRealReference, SW2_FutureReference, SWDataValue, \
    SWErrorReference, SW2_SweetheartReference,\
    SW2_FixedReference, SWReferenceJSONEncoder, SW2_ConcreteReference,\
    decode_datavalue_string, encode_datavalue, SW2_TombstoneReference
from ciel.public.io_helpers import read_framed_json, write_framed_json
from ciel.runtime.exceptions import BlameUserException, ReferenceUnavailableException,\
    MissingInputException, TaskFailedError
from ciel.runtime.block_store import get_own_netloc, filename_for_ref

from ciel.runtime.producer import make_local_output
from ciel.runtime.fetcher import fetch_ref_async
from ciel.runtime.object_cache import retrieve_object_for_ref, ref_from_object,\
    cache_object

import hashlib
import simplejson
import logging
import shutil
import subprocess
import tempfile
import os.path
import threading
import pickle
import time
import socket
import struct
from subprocess import PIPE
from datetime import datetime

from errno import EPIPE

import ciel
import traceback
import stat
import contextlib
import urllib2
import urlparse

running_children = {}

def add_running_child(proc):
    running_children[proc.pid] = proc

def remove_running_child(proc):
    del running_children[proc.pid]

def kill_all_running_children():
    for child in running_children.values():
        try:
            child.kill()
            child.wait()
        except:
            pass

class list_with:
    def __init__(self, l):
        self.wrapped_list = l
    def __enter__(self):
        return [x.__enter__() for x in self.wrapped_list]
    def __exit__(self, exnt, exnv, exntb):
        for x in self.wrapped_list:
            x.__exit__(exnt, exnv, exntb)
        return False

# Helper functions
def spawn_task_helper(task_record, executor_name, small_task=False, delegated_outputs=None, scheduling_class=None, scheduling_type=None, **executor_args):

    new_task_descriptor = {"handler": executor_name}
    if small_task:
        try:
            worker_private = new_task_descriptor["worker_private"]
        except KeyError:
            worker_private = {}
            new_task_descriptor["worker_private"] = worker_private
        worker_private["hint"] = "small_task"
    if scheduling_class is not None:
        new_task_descriptor["scheduling_class"] = scheduling_class
    if scheduling_type is not None:
        new_task_descriptor["scheduling_type"] = scheduling_type
    if delegated_outputs is not None:
        new_task_descriptor["expected_outputs"] = delegated_outputs
    return task_record.spawn_task(new_task_descriptor, is_tail_spawn=(delegated_outputs is not None), **executor_args)

def package_lookup(task_record, block_store, key):
    if task_record.package_ref is None:
        ciel.log.error("Package lookup for %s in task without package" % key, "EXEC", logging.WARNING)
        return None
    package_dict = retrieve_object_for_ref(task_record.package_ref, "pickle", task_record)
    try:
        return package_dict[key]
    except KeyError:
        ciel.log.error("Package lookup for %s: no such key" % key, "EXEC", logging.WARNING)
        return None

def multi_to_single_line(s):
    lines = s.split("\n")
    lines = filter(lambda x: len(x) > 0, lines)
    s = " // ".join(lines)
    if len(s) > 100:
        s = s[:99] + "..."
    return s

def test_program(args, friendly_name):
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (outstr, errstr) = proc.communicate()
        if proc.returncode == 0:
            ciel.log.error("Successfully tested %s: wrote '%s'" % (friendly_name, multi_to_single_line(outstr)), "EXEC", logging.INFO)
            return True
        else:
            ciel.log.error("Can't run %s: returned %d, stdout: '%s', stderr: '%s'" % (friendly_name, proc.returncode, outstr, errstr), "EXEC", logging.WARNING)
            return False
    except Exception as e:
        ciel.log.error("Can't run %s: exception '%s'" % (friendly_name, e), "EXEC", logging.WARNING)
        return False

def add_package_dep(package_ref, task_descriptor):
    if package_ref is not None:
        task_descriptor["dependencies"].append(package_ref)
        task_descriptor["task_private"]["package_ref"] = package_ref

def hash_update_with_structure(hash, value):
    """
    Recurses over a Skywriting data structure (containing lists, dicts and 
    primitive leaves) in a deterministic order, and updates the given hash with
    all values contained therein.
    """
    if isinstance(value, list):
        hash.update('[')
        for element in value:
            hash_update_with_structure(hash, element)
            hash.update(',')
        hash.update(']')
    elif isinstance(value, dict):
        hash.update('{')
        for (dict_key, dict_value) in sorted(value.items()):
            hash.update(dict_key)
            hash.update(':')
            hash_update_with_structure(hash, dict_value)
            hash.update(',')
        hash.update('}')
    elif isinstance(value, SWRealReference):
        hash.update('ref')
        hash.update(value.id)
    else:
        hash.update(str(value))

def accumulate_leaf_values(f, value):

    def flatten_lofl(ls):
        ret = []
        for l in ls:
            ret.extend(l)
        return ret

    if isinstance(value, list):
        accumd_list = [accumulate_leaf_values(f, v) for v in value]
        return flatten_lofl(accumd_list)
    elif isinstance(value, dict):
        accumd_keys = flatten_lofl([accumulate_leaf_values(f, v) for v in value.keys()])
        accumd_values = flatten_lofl([accumulate_leaf_values(f, v) for v in value.values()])
        accumd_keys.extend(accumd_values)
        return accumd_keys
    else:
        return [f(value)]

class ContextManager:
    def __init__(self, description):
        self.description = description
        self.active_contexts = []

    def add_context(self, new_context):
        ret = new_context.__enter__()
        self.active_contexts.append(ret)
        return ret
    
    def remove_context(self, context):
        self.active_contexts.remove(context)
        context.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            ciel.log("Context manager for %s exiting with exception %s" % (self.description, repr(exnv)), "EXEC", logging.WARNING)
        else:
            ciel.log("Context manager for %s exiting cleanly" % self.description, "EXEC", logging.DEBUG)
        for ctx in self.active_contexts:
            ctx.__exit__(exnt, exnv, exnbt)
        return False


class OngoingOutputWatch:
    
    def __init__(self, ongoing_output):
        self.ongoing_output = ongoing_output
        
    def start(self):
        self.ongoing_output.start_watch()
        
    def set_chunk_size(self, new_size):
        return self.ongoing_output.set_watch_chunk_size(new_size)
    
    def cancel(self):
        return self.ongoing_output.cancel_watch()

class OngoingOutput:

    def __init__(self, output_name, output_index, can_smart_subscribe, may_pipe, make_local_sweetheart, can_accept_fd, executor):
        kwargs = {"may_pipe": may_pipe, "can_use_fd": can_accept_fd}
        if can_smart_subscribe:
            kwargs["subscribe_callback"] = self.subscribe_output
        self.output_ctx = make_local_output(output_name, **kwargs)
        self.may_pipe = may_pipe
        self.make_local_sweetheart = make_local_sweetheart
        self.output_name = output_name
        self.output_index = output_index
        self.watch_chunk_size = None
        self.executor = executor
        self.filename = None
        self.fd = None

    def __enter__(self):
        return self

    def size_update(self, new_size):
        self.output_ctx.size_update(new_size)

    def close(self):
        self.output_ctx.close()

    def rollback(self):
        self.output_ctx.rollback()

    def get_filename(self):
        if self.filename is None:
            (self.filename, is_fd) = self.output_ctx.get_filename_or_fd()
            assert not is_fd
        return self.filename
    
    def get_filename_or_fd(self):
        if self.filename is None and self.fd is None:
            x, is_fd = self.output_ctx.get_filename_or_fd()
            if is_fd:
                self.fd = x
                return (self.fd, True)
            else:
                self.filename = x
                return (self.filename, False)
        else:
            if self.filename is None:
                return (self.fd, True)
            else:
                return (self.filename, False)
    
    def get_size(self):
        assert not self.may_pipe
        assert self.filename is not None
        return os.stat(self.filename).st_size
        
    def get_stream_ref(self):
        return self.output_ctx.get_stream_ref()

    def get_completed_ref(self):
        completed_ref = self.output_ctx.get_completed_ref()
        if isinstance(completed_ref, SW2_ConcreteReference) and self.make_local_sweetheart:
            completed_ref = SW2_SweetheartReference.from_concrete(completed_ref, get_own_netloc())
        return completed_ref

    def subscribe_output(self, _):
        return OngoingOutputWatch(self)

    def start_watch(self):
        self.executor._subscribe_output(self.output_index, self.watch_chunk_size)
        
    def set_watch_chunk_size(self, new_chunk_size):
        if self.watch_chunk_size is not None:
            self.executor._subscribe_output(self.output_index, new_chunk_size)
        self.watch_chunk_size = new_chunk_size

    def cancel_watch(self):
        self.watch_chunk_size = None
        self.executor._unsubscribe_output(self.output_index)

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            self.rollback()
        else:
            self.close()


