#!/usr/bin/python

import sys
from sys import stderr
import subprocess
import tempfile
import re
import datetime
import httplib
import simplejson
import os
import fcntl

time_regex = re.compile("^(\d+):(\d+):(\d+).(\d+)")

if len(sys.argv) < 2:
    sys.stderr.write("Usage: analyse_job_io.py master_address job_id")
    sys.exit(1)
if sys.argv[1] == "--help":
    print >>stderr, "Summarises the use of time in running a particular job, focussing on IO performance and how that effected the use of worker nodes' time."
    print >>stderr, "Usage: analyse_job_io.py master_address job_id"
    sys.exit(1)

conn = httplib.HTTPConnection(sys.argv[1])
conn.request("GET", "/worker/")
worker_list = simplejson.loads(conn.getresponse().read())
conn.close()

workers_by_id = dict()

for worker in worker_list:
    workers_by_id[worker["worker_id"]] = worker

workers_filename_fd, workers_filename = tempfile.mkstemp()

with tempfile.NamedTemporaryFile() as tasks_file:
    childpid = os.fork()
    if childpid == 0:
        os.dup2(tasks_file.fileno(), 1)
        os.dup2(workers_filename_fd, 3)
        flags = fcntl.fcntl(3, fcntl.F_GETFD)
        flags &= ~fcntl.FD_CLOEXEC
        fcntl.fcntl(3, fcntl.F_SETFD, flags)
        os.execv("./job_tasks.py", ["job_tasks.py"] + sys.argv[1:3])
    else:
        os.close(workers_filename_fd)
        os.waitpid(childpid, 0)

    worker_stats = dict()
        
    with open(workers_filename, "r") as workers_file:

        for worker in workers_file:
            print >>stderr, "Querying worker", worker[:-1]
            try:
                worker_netloc = workers_by_id[worker[:-1]]["netloc"]
            except KeyError:
                print >>stderr, "Failed to query worker", worker, "might have dissociated from master?"
            query_proc = subprocess.Popen(["./analyse_io.py", worker_netloc, tasks_file.name], stdout=subprocess.PIPE)
            in_summary = False
            for l in query_proc.stdout:
                if in_summary:
                    split_l = l.split()
                    desc = " ".join(split_l[:-1])
                    tm = time_regex.match(split_l[-1]).groups()
                    actual_tm = datetime.timedelta(hours=int(tm[0]), minutes=int(tm[1]), seconds=int(tm[2]), microseconds=int(tm[3]))
                    print >>stderr, desc, actual_tm
                    if desc not in worker_stats:
                        worker_stats[desc] = datetime.timedelta()
                    worker_stats[desc] += actual_tm
                else:
                    if l.find("Summary:") >= 0:
                        in_summary = True
            query_proc.wait()

        print >>stderr, "=============="
        print >>stderr, "Total summary:"

        for desc, tm in worker_stats.items():
            print desc, tm
