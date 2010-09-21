#!/usr/bin/python

import simplejson
import sys

input = sys.stdin.read()

tasks = simplejson.loads(input)

latest_toplevel_start = 0
latest_commit = 0

for task in tasks:
    if task["parent"] is None:
        if task["history"][0][1] != "CREATED":
            sys.stderr.write("Task %s's first history item is named %s, not CREATED!\n" % (task["task_id"], task["history"][0][1]))
            sys.exit(1)
        sys.stderr.write(("Task %s has no parent, and started at %f\n") % (task["task_id"], task["history"][0][0]))
        if task["history"][0][0] > latest_toplevel_start:
            latest_toplevel_start = task["history"][0][0]
    if task["history"][-1][1] != "COMMITTED":
        sys.stderr.write("Task %s's last history item is named %s, not COMMITTED!\n" % (task["task_id"], task["history"][-1][1]))
    if task["history"][-1][0] > latest_commit:
        latest_commit = task["history"][-1][0]

sys.stderr.write("Latest task started at %f and ended at %f\n" % (latest_toplevel_start, latest_commit))
print (latest_commit - latest_toplevel_start)
                         
