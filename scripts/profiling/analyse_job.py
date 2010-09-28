#!/usr/bin/python

import simplejson
import sys
from optparse import OptionParser

input = sys.stdin.read()

tasks = simplejson.loads(input)

latest_toplevel_start = 0
latest_commit = 0

num_committed = 0

parser = OptionParser()
parser.add_option("-d", "--duration", action="store_true", dest="get_duration",
                  help="extract total job execution duration", default=False)
parser.add_option("-c", "--commitstats", action="store_true", 
                  dest="get_commit_count", default=False,
                  help="print commit stats in gnuplot format")
parser.add_option("-a", "--allstats", action="store_true", 
                  dest="get_all", default=False,
                  help="print all stats in gnuplot format")

(options, args) = parser.parse_args()

committed_tasks = []
task_events = []

eventcounters = { "CREATED": 0, "RUNNING": 0, "BLOCKING": 0, "RUNNABLE": 0, "QUEUED": 0, "ASSIGNED": 0, "COMMITTED": 0, "MISSING_INPUT": 0, "FAILED": 0 }
taskstate = {}

for task in tasks:
    if task["parent"] is None:
        # this is a root task, or it has weird log data
        if task["history"][0][1] != "CREATED":
            sys.stderr.write("Task %s's first history item is named %s, not CREATED!\n" % (task["task_id"], task["history"][0][1]))
            sys.exit(1)
        # we ignore root tasks other than the one specified on the command line argument, if set
        if len(sys.argv) > 1 and task["task_id"] == sys.argv[len(sys.argv)-1]:
            # found the correct root task
            sys.stderr.write("Found root task with id %s\n" % task["task_id"])
            latest_toplevel_start = task["history"][0][0]
        elif len(sys.argv) > 1 and task["task_id"] != sys.argv[len(sys.argv)-1]:
            # some other root task, skip
            continue
        else:
            # no command line argument, output all root tasks
            sys.stderr.write(("Found root task %s, which started at %f\n") % (task["task_id"], task["history"][0][0]))
            if task["history"][0][0] > latest_toplevel_start:
                latest_toplevel_start = task["history"][0][0]

    if task["history"][-1][1] != "COMMITTED":
        # non-committed task - output last state
        sys.stderr.write("Task %s's last history item is named %s, not COMMITTED!\n" % (task["task_id"], task["history"][-1][1]))

    if task["history"][-1][0] > latest_commit:
        latest_commit = task["history"][-1][0]

    # process task data and output it
#    if options.dump_task_data:
#        pass

    # output committed task stats
    if options.get_commit_count:
        if task["history"][-1][1] == "COMMITTED":
            committed_tasks.append((task["history"][-1][0], task["task_id"]))
    elif options.get_all:
        for evt in task["history"]:
            task_events.append((evt[0], task["task_id"], evt[1]))

if options.get_commit_count:
    committed_tasks.sort()
    for (time, tid) in committed_tasks:
         num_committed += 1
         sys.stderr.write("%f\t%s\t%d\n" % (time, tid, num_committed))
elif options.get_all:
    task_events.sort()
    for (time, tid, event) in task_events:
         try:
            eventcounters[taskstate[tid]] -= 1
            eventcounters[event] += 1
            taskstate[tid] = event
         except KeyError:
            taskstate[tid] = event
            eventcounters[event] += 1
         sys.stderr.write("%f\t%s\t%d\t%d\t%d\t%d\t%d\n" % (time, tid, eventcounters["CREATED"], eventcounters["ASSIGNED"], eventcounters["RUNNING"], eventcounters["BLOCKING"], eventcounters["COMMITTED"]))
    
# output job duration
if options.get_duration:
    sys.stderr.write("Latest task started at %f and ended at %f\n" % (latest_toplevel_start, latest_commit))
    print "Total duration: " + str((latest_commit - latest_toplevel_start)) + "s"

