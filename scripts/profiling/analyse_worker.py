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

datetime_regex = re.compile("datetime\.datetime\((\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)\)")
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

    event_time = datetime_regex.match(log_record["time"])
    matched_elements = event_time.groups()
    real_time = datetime.datetime(int(matched_elements[0]), int(matched_elements[1]), int(matched_elements[2]), int(matched_elements[3]), int(matched_elements[4]), int(matched_elements[5]), int(matched_elements[6]))
    event_text = log_record["event"]

    if event_text.find("Start execution") >= 0:
        start_time = real_time
        try:
            this_idle_time = start_time - previous_state_time
            this_idle_time_float = (float(this_idle_time.seconds) + (float(this_idle_time.microseconds) / 1000000))
            sys.stdout.write("    ")
            print "Idle for", this_idle_time_float, "seconds"
            idle_time_float += this_idle_time_float
        except:
            pass
        previous_state_time = real_time
        current_state = "misc"
        event_uuid_match = uuid_regex.search(event_text)
        task_uuid = event_uuid_match.groups()[0]
    elif event_text.find("Block store: fetching") >= 0:
        misses.append(event_text[22:])
    elif event_text.find("local already") >= 0 or event_text.find("hit in cache") >= 0:
        hits.append("blah")
    elif event_text.find("Fetching args") >= 0:
        change_state("init_fetch", real_time)
    elif event_text.find("Interpreting") >= 0:
        change_state("interpret", real_time)
    elif event_text.find("Committing") >= 0:
        change_state("commit", real_time)
    elif event_text.find("Executing") >= 0:
        change_state("input_fetch", real_time)
    elif event_text.find("Java: running") >= 0:
        change_state("execute", real_time)
    elif event_text.find("Java: Storing outputs") >= 0:
        change_state("output_store", real_time)
    elif event_text.find("Completed execution") >= 0:
        change_state("done", real_time)
        task_time = real_time - start_time
        print "Task", task_uuid, "took", str(task_time), ("(%d hits, %d misses)" % (len(hits), len(misses)))
        task_time_float = float(task_time.seconds) + (float(task_time.microseconds) / 1000000)
        sys.stdout.write("    ")
        for (state_name, state_time) in task_state_times.iteritems():
            state_time_percent = (state_time / task_time_float) * 100
            sys.stdout.write("%s %.3g%%   " % (state_name, state_time_percent))
        sys.stdout.write("\n")
        start_time = None
        task_uuid = None
        misses = []
        hits = []
        task_state_times = dict()

sys.stdout.write("Summary: ")
total_time = 0.0
for (state_name, state_time) in state_times.iteritems():
    total_time += state_time
total_time += idle_time_float
print "Total elapsed time", total_time, "seconds"
for (state_name, state_time) in (state_times.items() + [("idle", idle_time_float)]):
    state_time_percent = (state_time / total_time) * 100
    sys.stdout.write("%s %.3g%%   " % (state_name, state_time_percent))
sys.stdout.write("\n")


