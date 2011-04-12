
# Requires PyPy or Stackless Python

from __future__ import with_statement

import optparse
import stackless
import pickle
import imp
import sys
import os
import simplejson

import skypy
from rpc_helper import RpcHelper
from file_outputs import FileOutputRecords

from shared.io_helpers import MaybeFile
from shared.references import SW2_FutureReference

parser = optparse.OptionParser()
parser.add_option("-v", "--version", action="store_true", dest="version", default=False, help="Display version info")

(options, args) = parser.parse_args()

if options.version:
    print "Ciel SkyPy v0.1. Python:"
    print sys.version
    sys.exit(0)

print >>sys.stderr, "SkyPy: Awaiting task"
unbuffered_stdin = os.fdopen(0, "r", 0)
entry_dict = pickle.load(unbuffered_stdin)

skypy.main_coro = stackless.coroutine.getcurrent()

with skypy.deref_as_raw_file(entry_dict["py_ref"]) as py_file:
    source_filename = py_file.filename
user_script_namespace = imp.load_source("user_script_namespace", source_filename)

if "coro_ref" in entry_dict:
    print >>sys.stderr, "SkyPy: Resuming"
    with skypy.deref_as_raw_file(entry_dict["coro_ref"]) as coro_fp:
        coro_filename = coro_fp.filename
    resume_fp = open(coro_filename, "r")
    resume_state = pickle.load(resume_fp)
    resume_fp.close()
    user_coro = resume_state.coro
else:
    print >>sys.stderr, "Entering at", entry_dict["entry_point"], "args", entry_dict["entry_args"]
    skypy.persistent_state = skypy.PersistentState()
    user_coro = stackless.coroutine()
    user_coro.bind(skypy.start_script, user_script_namespace.__dict__[entry_dict["entry_point"]], entry_dict["entry_args"])
    resume_state = skypy.ResumeState(skypy.persistent_state, user_coro)
if not entry_dict["is_continuation"]:
    resume_state.persistent_state = skypy.PersistentState()
    resume_state.persistent_state.export_json = entry_dict["export_json"]
    resume_state.persistent_state.ret_output = entry_dict["ret_output"]
    resume_state.persistent_state.extra_outputs = entry_dict["extra_outputs"]
    resume_state.persistent_state.py_ref = entry_dict["py_ref"]
skypy.persistent_state = resume_state.persistent_state
skypy.extra_outputs = skypy.persistent_state.extra_outputs

skypy.file_outputs = FileOutputRecords()
skypy.message_helper = RpcHelper(unbuffered_stdin, sys.stdout, skypy.file_outputs)
skypy.file_outputs.set_message_helper(skypy.message_helper)

user_coro.switch()
# We're back -- either the user script is done, or else it's stuck waiting on a reference.
if skypy.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
    report = "Runtime exception %s\n%s" % (str(skypy.script_return_val), skypy.script_backtrace)
    out_dict = {"request": "exception",
                "report": report}
else:
    if skypy.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
        coro_ref = skypy.save_state(resume_state)
        out_dict = {"request": "tail_spawn",
                    "handler": "skypy",
                    "py_ref": skypy.persistent_state.py_ref,
                    "coro_ref": coro_ref,
                    "additional_deps": [SW2_FutureReference(x) for x in skypy.persistent_state.ref_dependencies.keys()]}
        pickle.dump(out_dict, sys.stdout)
    elif skypy.halt_reason == skypy.HALT_DONE:
        out_fp = MaybeFile(open_callback=skypy.open_output(skypy.persistent_state.ret_output))
        with out_fp:
            if skypy.persistent_state.export_json:
                simplejson.dump(skypy.script_return_val, out_fp)
            else:
                pickle.dump(skypy.script_return_val, out_fp)
        skypy.ref_from_maybe_file(out_fp, skypy.persistent_state.ret_output)
    out_dict = {"request": "done"}



