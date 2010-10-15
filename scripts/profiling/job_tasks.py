#!/usr/bin/python

import sys
import httplib
import simplejson
from sys import stderr

if len(sys.argv) < 2:
    sys.stderr.write("Usage: job_tasks.py master_address job_id")
    sys.exit(1)

conn = httplib.HTTPConnection(sys.argv[1])
conn.request("GET", "/task/")
task_list = simplejson.loads(conn.getresponse().read())
conn.close()

tasks_by_id = dict()

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
    for child in task["children"]:
        print >>stderr, level, child
        print child
        recursive_enumerate_tasks(level + 1, tasks_by_id[child])

recursive_enumerate_tasks(1, root_task)





