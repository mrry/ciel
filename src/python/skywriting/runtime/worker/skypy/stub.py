
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

parser = optparse.OptionParser()
parser.add_option("-r", "--resume_state", dest="state_file", 
                  help="load state image from FILE", metavar="FILE")
parser.add_option("-s", "--source", dest="source_file",
                  help="load user source from FILE", metavar="FILE")
parser.add_option("-t", "--taskid", dest="taskid",
                  help="task ID to use generating spawned task names", metavar="ID")

sys.stderr.write("SkyPy: Started with args %s\n" % sys.argv)

(options, args) = parser.parse_args()

resume_file = None
try:
    resume_file = options.state_file
except:
    pass
source_file = options.source_file
skypy.taskid = options.taskid

skypy.main_coro = stackless.coroutine.getcurrent()
skypy.runtime_out = sys.stdout
skypy.runtime_in = sys.stdin
user_script_namespace = imp.load_source("user_script_namespace", source_file)

if resume_file is not None:
    print >>sys.stderr, "SkyPy: Resuming"
    resume_fp = open(resume_file, "r")
    resume_state = pickle.load(resume_fp)
    resume_fp.close()

    skypy.persistent_state = resume_state.persistent_state
    user_coro = resume_state.coro
    user_coro.switch()
    # We're back -- either the user script is done, or else it's stuck waiting on a reference.
    output_fp = skypy.MaybeFile()
    if skypy.halt_reason == skypy.HALT_REFERENCE_UNAVAILABLE:
        pickle.dump(resume_state, output_fp)
        out_dict = {"request": "freeze", 
                    "additional_deps": skypy.persistent_state.ref_dependencies}
    elif skypy.halt_reason == skypy.HALT_DONE:
        pickle.dump(skypy.script_return_val, output_fp)
        out_dict = {"request": "done"}
    elif skypy.halt_reason == skypy.HALT_RUNTIME_EXCEPTION:
        pickle.dump("Runtime exception %s\n%s" % (str(skypy.script_return_val), skypy.script_backtrace), output_fp)
        out_dict = {"request": "exception"}
    skypy.describe_maybe_file(output_fp, out_dict)
    pickle.dump(out_dict, sys.stdout)

else:
    try:
        print >>sys.stderr, "SkyPy: Creating initial coroutine"
        skypy.initial_run_args = args
        user_coro = stackless.coroutine()
        user_coro.bind(skypy.freeze_script_at_startup, user_script_namespace.skypy_main)
        user_coro.switch()
        resume_state = skypy.PersistentState()
        pickle.dump(skypy.ResumeState(resume_state, user_coro), sys.stdout)
    except Exception, e:
        print >>sys.stderr, "PyPy failed to create initial continuation:"
        print >>sys.stderr, traceback.format_exc()
        sys.exit(1)

