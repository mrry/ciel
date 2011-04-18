
# Requires PyPy or Stackless Python

from __future__ import with_statement

import optparse
import stackless
import pickle
import imp
import sys
import simplejson

import skypy
from shared.rpc_helper import RpcHelper, ShutdownException
from file_outputs import FileOutputRecords

from shared.io_helpers import MaybeFile, write_framed_json
from shared.references import SW2_FutureReference
import traceback

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

main_coro = stackless.coroutine.getcurrent()

while True:

    try:
        file_outputs = FileOutputRecords()
        message_helper = RpcHelper(read_fp, write_fp, file_outputs)
        file_outputs.set_message_helper(message_helper)
        
        print >>sys.stderr, "SkyPy: Awaiting task"
        entry_dict = message_helper.await_message("start_task")

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
            persistent_state = skypy.PersistentState(export_json=entry_dict["export_json"],
                                                     extra_outputs=entry_dict["extra_outputs"],
                                                     py_ref=entry_dict["py_ref"])
            resume_state.persistent_state = persistent_state
            user_coro = stackless.coroutine()
            user_coro.bind(skypy.start_script, user_script_namespace.__dict__[entry_dict["entry_point"]], entry_dict["entry_args"])
            resume_state = skypy.ResumeState(persistent_state, user_coro)

        skypy.current_task = skypy.SkyPyTask(main_coro, 
                                             resume_state.persistent_state,
                                             message_helper,
                                             file_outputs)

        user_coro.switch()
        # We're back -- either the user script is done, or else it's stuck waiting on a reference.
        if skypy.current_task.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
            report = "User script exception %s\n%s" % (str(skypy.current_task.script_return_val), skypy.current_task.script_backtrace)
            out_message = ("error", {"report": report})
        else:
            if skypy.current_task.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
                coro_ref = skypy.save_state(resume_state)
                out_message = ("tail_spawn", 
                                {"executor_name": "skypy",
                                 "pyfile_ref": skypy.current_task.persistent_state.py_ref,
                                 "coro_ref": coro_ref,
                                 "extra_dependencies": [SW2_FutureReference(x) for x in skypy.current_task.persistent_state.ref_dependencies.keys()]
                                 }
                               )
                write_framed_json(out_message, write_fp)
            elif skypy.current_task.halt_reason == skypy.HALT_DONE:
                out_fp = MaybeFile(open_callback=lambda: skypy.open_output(0))
                with out_fp:
                    if skypy.current_task.persistent_state.export_json:
                        simplejson.dump(skypy.current_task.script_return_val, out_fp)
                    else:
                        pickle.dump(skypy.current_task.script_return_val, out_fp)
                skypy.ref_from_maybe_file(out_fp, 0)
            out_message = ("exit", {"keep_process": "may_keep"})
        write_framed_json(out_message, write_fp)
    except ShutdownException as e:
        print >>sys.stderr, "SkyPy: killed by Ciel (reason: '%s')" % e.reason
        sys.exit(0)
    except Exception as e:
        print >>sys.stderr, "SkyPy: exception reached top level!"
        report = "Top-level exception %s\n%s" % (repr(e), traceback.format_exc())
        write_framed_json(out_message, write_fp)
        sys.exit(1)        