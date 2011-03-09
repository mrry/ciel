
# Requires PyPy or Stackless Python

from __future__ import with_statement

import optparse
import stackless
import pickle
import imp
import sys
import tempfile
import traceback
import os

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
entry_dict = pickle.load(sys.stdin)

skypy.main_coro = stackless.coroutine.getcurrent()
user_script_namespace = imp.load_source("user_script_namespace", entry_dict["source_filename"])

if "coro_filename" in entry_dict:
    print >>sys.stderr, "SkyPy: Resuming"
    resume_fp = open(entry_dict["coro_filename"], "r")
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
skypy.persistent_state = resume_state.persistent_state
skypy.extra_outputs = skypy.persistent_state.extra_outputs

skypy.file_outputs = FileOutputRecords()
skypy.message_helper = RpcHelper(sys.stdin, sys.stdout, skypy.file_outputs)
skypy.file_outputs.set_message_helper(skypy.message_helper)

user_coro.switch()
# We're back -- either the user script is done, or else it's stuck waiting on a reference.
with MaybeFile() as output_fp:
    if skypy.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
        pickle.dump(resume_state, output_fp)
        out_dict = {"request": "freeze", 
                    "additional_deps": [SW2_FutureReference(x) for x in skypy.persistent_state.ref_dependencies.keys()]}
    elif skypy.halt_reason == skypy.HALT_DONE:
        pickle.dump(skypy.script_return_val, output_fp)
        out_dict = {"request": "done",
                    "ret_output": skypy.persistent_state.ret_output,
                    "export_json": skypy.persistent_state.export_json}
    elif skypy.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
        pickle.dump("Runtime exception %s\n%s" % (str(skypy.script_return_val), skypy.script_backtrace), output_fp)
        out_dict = {"request": "exception"}
    skypy.describe_maybe_file(output_fp, out_dict)
pickle.dump(out_dict, sys.stdout)



