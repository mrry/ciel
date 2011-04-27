
# Requires PyPy or Stackless Python

from __future__ import with_statement

import optparse
import stackless
import pickle
import imp
import sys
import simplejson

import skypy
import soft_cache

from shared.rpc_helper import RpcHelper, ShutdownException
from file_outputs import FileOutputRecords

from shared.io_helpers import MaybeFile, write_framed_json
from shared.references import SW2_FutureReference, decode_datavalue_string
import traceback

from StringIO import StringIO

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

user_script_namespaces = dict()

main_coro = stackless.coroutine.getcurrent()

while True:

    try:
        
        soft_cache.garbage_collect_cache()
        
        file_outputs = FileOutputRecords()
        message_helper = RpcHelper(read_fp, write_fp, file_outputs)
        file_outputs.set_message_helper(message_helper)
        
        print >>sys.stderr, "SkyPy: Awaiting task"
        entry_dict = message_helper.await_message("start_task")

        if skypy.current_task is None: # Otherwise we're in fixed mode -- this new task continues the old one
            
            user_script_namespace = user_script_namespaces.get(entry_dict["py_ref"].id, None)
            if user_script_namespace is None:
                runtime_response = skypy.fetch_ref(entry_dict["py_ref"], "open_ref", message_helper, make_sweetheart=True)
                source_filename = runtime_response["filename"]
                    
                user_script_namespace = imp.load_source(str("user_namespace_%s" % entry_dict["py_ref"].id), source_filename)
                user_script_namespaces[entry_dict["py_ref"].id] = user_script_namespace
            else:
                print >>sys.stderr, "SkyPy: Using pre-parsed .py file"
    
            if "coro_ref" in entry_dict:
                print >>sys.stderr, "SkyPy: Resuming"
                resume_state = soft_cache.try_get_cache([entry_dict["coro_ref"]], "coro")
                if resume_state is not None:
                    print >>sys.stderr, "SkyPy: Resuming from soft-cached coroutine"
                else:
                    runtime_response = skypy.fetch_ref(entry_dict["coro_ref"], "open_ref", message_helper, make_sweetheart=True)
                    if "strdata" in runtime_response:
                        fp = StringIO(decode_datavalue_string(runtime_response["strdata"]))
                    else:
                        fp = open(runtime_response["filename"], "r")
                    with fp:
                        resume_state = pickle.load(fp)
                user_coro = resume_state.coro
            else:
                print >>sys.stderr, "Entering at", entry_dict["entry_point"], "args", entry_dict["entry_args"]
                persistent_state = skypy.PersistentState(export_json=entry_dict["export_json"],
                                                         extra_outputs=entry_dict["extra_outputs"],
                                                         py_ref=entry_dict["py_ref"],
                                                         is_fixed=entry_dict["run_fixed"])
                user_coro = stackless.coroutine()
                user_coro.bind(skypy.start_script, user_script_namespace.__dict__[entry_dict["entry_point"]], entry_dict["entry_args"])
                resume_state = skypy.ResumeState(persistent_state, user_coro)
    
            user_script_namespace = None

            skypy.current_task = skypy.SkyPyTask(main_coro, 
                                                 resume_state.persistent_state,
                                                 message_helper,
                                                 file_outputs)
        else:
            
            print >>sys.stderr, "SkyPy: continuing previous fixed task"

        while True:
            user_coro.switch()
            if skypy.current_task.main_coro_callback is not None:
                skypy.current_task.main_coro_callback_response = skypy.current_task.main_coro_callback()
                skypy.current_task.main_coro_callback = None
            else:
                break
        # We're back -- either the user script is done, or else it's stuck waiting on a reference.
        if skypy.current_task.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
            report = "User script exception %s\n%s" % (str(skypy.current_task.script_return_val), skypy.current_task.script_backtrace)
            out_message = ("error", {"report": report})
        else:
            if skypy.current_task.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
                out_dict = {"executor_name": "skypy",
                            "extra_dependencies": [SW2_FutureReference(x) for x in skypy.current_task.persistent_state.ref_dependencies.keys()],
                            "is_fixed": skypy.current_task.persistent_state.is_fixed
                           }
                if not skypy.current_task.persistent_state.is_fixed:
                    coro_ref = skypy.save_state(resume_state, make_local_sweetheart=True)
                    soft_cache.put_cache([coro_ref], "coro", resume_state)
                    out_dict.update({"pyfile_ref": skypy.current_task.persistent_state.py_ref,
                                     "coro_ref": coro_ref})
                write_framed_json(("tail_spawn", out_dict), write_fp)
            elif skypy.current_task.halt_reason == skypy.HALT_DONE:
                out_fp = MaybeFile(open_callback=lambda: skypy.open_output(0))
                with out_fp:
                    if skypy.current_task.persistent_state.export_json:
                        simplejson.dump(skypy.current_task.script_return_val, out_fp)
                    else:
                        pickle.dump(skypy.current_task.script_return_val, out_fp)
                skypy.ref_from_maybe_file(out_fp, 0)
            if skypy.current_task.persistent_state.is_fixed:
                out_message = ("exit", {"keep_process": "must_keep"})
            else:
                out_message = ("exit", {"keep_process": "may_keep", "soft_cache_keys": soft_cache.get_cache_keys()})
        write_framed_json(out_message, write_fp)
        if skypy.current_task.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
            sys.exit(0)
        if not skypy.current_task.persistent_state.is_fixed:
            skypy.current_task = None
    except ShutdownException, e:
        print >>sys.stderr, "SkyPy: killed by Ciel (reason: '%s')" % e.reason
        sys.exit(0)
    except Exception, e:
        print >>sys.stderr, "SkyPy: exception reached top level!"
        report = "Top-level exception %s\n%s" % (repr(e), traceback.format_exc())
        print >>sys.stderr, report
        out_message = ("error", {"report": report})
        write_framed_json(out_message, write_fp)
        sys.exit(1)        