#!/usr/bin/python

import sys
import os
import httplib
import simplejson
from sys import stderr

if len(sys.argv) < 2:
    sys.stderr.write("Usage: job_tasks.py master_address job_id")
    sys.exit(1)
try:
    workers_out = os.fdopen(3, "w")
except Exception as e:
    print >>stderr, "Must run this script with FD 3 pointed somewhere"
    print >>stderr, e
    sys.exit(1)
if sys.argv[1] == "--help":
    print >>stderr, "Writes the task IDs involved in a particular job to stdout, and the worker IDs involved to FD 3."
    print >>stderr, "Usage: job_tasks.py master_address job_id"
    sys.exit(1)

conn = httplib.HTTPConnection(sys.argv[1])
conn.request("GET", "/task/")
task_list = simplejson.loads(conn.getresponse().read())
conn.close()

tasks_by_id = dict()
workers_reported = set()

for task in task_list:
    tasks_by_id[task["task_id"]] = task

root_task = None
try:
    root_task = tasks_by_id["root:" + sys.argv[2]]
except:
    print "Can't find task", ("root:" + sys.argv[2])
    sys.exit(1)

print >>stderr, "Root task is", root_task["task_id"]

def recursive_enumerate_tasks(level, task):
    global workers_reported
    print >>stderr, level, task["task_id"], "worker", task["worker_id"]
    print task["task_id"]
    if task["worker_id"] not in workers_reported:
        workers_reported.add(task["worker_id"])
        print >>workers_out, task["worker_id"]
    for child in task["children"]:
        recursive_enumerate_tasks(level + 1, tasks_by_id[child])

recursive_enumerate_tasks(1, root_task)





