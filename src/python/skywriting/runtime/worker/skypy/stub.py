
# Requires PyPy or Stackless Python

from __future__ import with_statement

import optparse
import stackless
import pickle
import imp
import sys
import simplejson

import skypy
from rpc_helper import RpcHelper
from file_outputs import FileOutputRecords

from shared.io_helpers import MaybeFile, read_framed_json, write_framed_json
from shared.references import SW2_FutureReference

parser = optparse.OptionParser()
parser.add_option("-v", "--version", action="store_true", dest="version", default=False, help="Display version info")
parser.add_option("-w", "--write-fifo", action="store", dest="write_fifo", default=None, help="FIFO for communication towards Ciel")
parser.add_option("-r", "--read-fifo", action="store", dest="read_fifo", default=None, help="FIFO for communication from Ciel")

(options, args) = parser.parse_args()

if options.version:
    print "Ciel SkyPy v0.1. Python:"
    print sys.version
    sys.exit(0)
    
if options.write_fifo is None or options.read_fifo is None:
    print >>sys.stderr, "SkyPy: Must specify a read-fifo and a write-fifo."
    sys.exit(1)

write_fp = open(options.write_fifo, "w")
read_fp = open(options.read_fifo, "r", 0)
# Unbuffered so we can use select() on its FD for IO mux

print >>sys.stderr, "SkyPy: Awaiting task"
(first_method, entry_dict) = read_framed_json(read_fp)
if first_method != "start_task":
    print >>sys.stderr, "SkyPy: First method was not 'start_task'"
    sys.exit(1)

skypy.main_coro = stackless.coroutine.getcurrent()

skypy.file_outputs = FileOutputRecords()
skypy.message_helper = RpcHelper(read_fp, write_fp, skypy.file_outputs)
skypy.file_outputs.set_message_helper(skypy.message_helper)

with skypy.deref_as_raw_file(entry_dict["py_ref"]) as py_file:
    source_filename = py_file.filename
user_script_namespace = imp.load_source("user_script_namespace", source_filename)

if "coro_ref" in entry_dict:
    print >>sys.stderr, "SkyPy: Resuming"
    with skypy.deref_as_raw_file(entry_dict["coro_ref"]) as resume_fp:
        resume_state = pickle.load(resume_fp)
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

user_coro.switch()
# We're back -- either the user script is done, or else it's stuck waiting on a reference.
if skypy.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
    report = "Runtime exception %s\n%s" % (str(skypy.script_return_val), skypy.script_backtrace)
    out_message = ("error", {"report": report})
else:
    if skypy.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
        coro_ref = skypy.save_state(resume_state)
        out_message = ("tail_spawn", 
                        {"executor_name": "skypy",
                         "pyfile_ref": skypy.persistent_state.py_ref,
                         "coro_ref": coro_ref,
                         "extra_dependencies": [SW2_FutureReference(x) for x in skypy.persistent_state.ref_dependencies.keys()]
                         }
                       )
        write_framed_json(out_message, write_fp)
    elif skypy.halt_reason == skypy.HALT_DONE:
        out_fp = MaybeFile(open_callback=lambda: skypy.open_output(skypy.persistent_state.ret_output))
        with out_fp:
            if skypy.persistent_state.export_json:
                simplejson.dump(skypy.script_return_val, out_fp)
            else:
                pickle.dump(skypy.script_return_val, out_fp)
        skypy.ref_from_maybe_file(out_fp, skypy.persistent_state.ret_output)
    out_message = ("exit", {"keep_process": False})
write_framed_json(out_message, write_fp)