#!/usr/bin/python

import sys
import httplib
import simplejson
import re
import datetime

if len(sys.argv) < 2:
    sys.stderr.write("Usage: analyser_worker.py worker_address")
    sys.exit(1)

conn = httplib.HTTPConnection(sys.argv[1])
conn.request("GET", "/log/?first_event=0&last_event=1000")
log_records = simplejson.loads(conn.getresponse().read())
conn.close()

uuid_regex = re.compile("'([^']*)'")

start_time = None
task_uuid = None
previous_state_time = None
misses = []
hits = []
current_state = "misc"
state_times = dict()
task_state_times = dict()
idle_time_float = 0.0

def change_state(new_state, current_time):
    global state_times
    global task_state_times
    global previous_state_time
    global current_state
    if current_state not in state_times:
        state_times[current_state] = 0.0
    if current_state not in task_state_times:
        task_state_times[current_state] = 0.0
    state_time = current_time - previous_state_time
    previous_state_time = current_time
    state_time_float = float(state_time.seconds) + (float(state_time.microseconds) / 1000000)
    state_times[current_state] += state_time_float
    task_state_times[current_state] += state_time_float
    current_state = new_state

for log_record in log_records:

    event_text = log_record["event"]
    if event_text.find("Process log") >= 0:
        print "Process log entry:", event_text
    elif event_text.find("Fetch trace") >= 0:
        print "Fetch trace entry:", event_text
    elif event_text.find("Fetch FIFO trace") >= 0:
        print "Fetch FIFO trace entry:", event_text

