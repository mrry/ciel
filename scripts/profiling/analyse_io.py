#!/usr/bin/python

import sys
import httplib
import simplejson
import re
from datetime import datetime, timedelta
from sys import stderr

if len(sys.argv) < 2:
    sys.stderr.write("Usage: analyser_worker.py worker_address [interesting_task_file]")
    sys.exit(1)

conn = httplib.HTTPConnection(sys.argv[1])
conn.request("GET", "/log/?first_event=0&last_event=1000000")
log_records = simplejson.loads(conn.getresponse().read())
conn.close()

datetime_regex = re.compile("datetime\.datetime\((\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)\)")
uuid_regex = re.compile("'([^']*)'")

tasks = dict()
current_task = None
current_task_name = None

def fifo_fetch_comparator(x, y):
    (t1, _) = x
    (t2, _) = y
    return cmp(t1, t2)

def datetime_from_text(t):

    event_time = datetime_regex.match(t)
    matched_elements = event_time.groups()
    return datetime(int(matched_elements[0]), int(matched_elements[1]), int(matched_elements[2]), int(matched_elements[3]), int(matched_elements[4]), int(matched_elements[5]), int(matched_elements[6]))

class TaskRecord:

    def __init__(self, start_time):
        self.completed = False
        self.fetch_logs = dict()
        self.fifo_logs = dict()
        self.state_log = []
        self.start_time = datetime_from_text(start_time)
        self.process_log = [(self.start_time, "Start")]
        self.end_time = None
        self.task_uuid = None
        self.previous_state_time = None
        self.misses = []
        self.hits = []
        self.current_state = "misc"
        self.state_times = dict()
        self.task_state_times = dict()
        self.idle_time_float = 0.0

    def _add_timed_log(self, log, message, t):
        (prev_time_begin, prev_message) = log[-1]
        log.pop()
        log.append(((prev_time_begin, t), prev_message))
        log.append((t, message))

    def add_timed_log(self, log, message):
        split_message = message.split()
        event_time = datetime.fromtimestamp(float(split_message[0]))
        message = " ".join(split_message[1:])
        self._add_timed_log(log, message, event_time)

    def add_process_log(self, message):
        self.add_timed_log(self.process_log, message)

    def add_indexed_log(self, log, message):
        message_split = message.split()
        fetch_id = int(message_split[0])
        if fetch_id not in log:
            log[fetch_id] = [(self.start_time, "Start")]
        self.add_timed_log(log[fetch_id], " ".join(message_split[1:]))

    def add_fetch_log(self, message):
        self.add_indexed_log(self.fetch_logs, message)
        
    def add_fifo_log(self, message):
        self.add_indexed_log(self.fifo_logs, message)

    def mark_done(self, t):
        self.completed = True
        self.end_time = datetime_from_text(t)
        self._add_timed_log(self.process_log, "Finished", self.end_time)
        self.process_log = self.process_log[:-1]
        for id in self.fetch_logs:
            self._add_timed_log(self.fetch_logs[id], "Finished", self.end_time)
            self.fetch_logs[id].pop()
        for id in self.fifo_logs:
            self._add_timed_log(self.fifo_logs[id], "Finished", self.end_time)
            self.fifo_logs[id].pop()

    def add_time(self, state, t1, t2):
        self.state_log.append(((t1, t2), state))

    def overlapping_time(self, t1, t2, t1p, t2p):
        start_t = None
        if t1 < t1p:
            start_t = t1p
        else:
            start_t = t1
        end_t = None
        if t2 < t2p:
            end_t = t2
        else:
            end_t = t2p
        return (start_t, end_t)

    def report_fetch_time(self, wait_id, t1, t2):
        for ((ev_start, ev_end), state) in self.fetch_logs[wait_id]:
            if ev_start > t2:
                return
            elif ev_end < t1:
                continue
            else:
                # At least some overlap
                (start_t, end_t) = self.overlapping_time(t1, t2, ev_start, ev_end)
                if state.find("receiving") >= 0:
                    self.add_time("Consuming input %d (unbuffered)" % wait_id, start_t, end_t)
                elif state.find("Request-sent") >= 0:
                    self.add_time("Waiting for reply input %d" % wait_id, start_t, end_t)
                elif state.find("Start") >= 0:
                    self.add_time("Waiting for first request input %d" % wait_id, start_t, end_t)
                elif state.find("Dormant") >= 0:
                    self.add_time("Waiting for slow producer input %d" % wait_id, start_t, end_t)
                elif state.find("EOF") >= 0:
                    self.add_time("EOF input %d" % wait_id, start_t, end_t)

    def report_fifo_time(self, wait_id, t1, t2):

        if wait_id not in self.fifo_logs:
            self.add_time("Reading cached input %d" % wait_id, t1, t2)
        else:
            for ((ev_start, ev_end), state) in self.fifo_logs[wait_id]:
                if ev_start > t2:
                    return
                elif ev_end < t1:
                    continue
                else:
                    # At least some overlap
                    (start_t, end_t) = self.overlapping_time(t1, t2, ev_start, ev_end)
                    if state.find("Buffer not empty") >= 0:
                        self.add_time("Consuming input %d (buffered)" % wait_id, start_t, end_t)
                    elif state.find("EOF") >= 0:
                        self.add_time("EOF input %d" % wait_id, start_t, end_t)
                    elif state.find("Start") >= 0:
                        self.add_time("Waiting for FIFO to attach input %d" % wait_id, start_t, end_t)
                    else:
                        self.report_fetch_time(wait_id, start_t, end_t)

    def produce_time_series(self):

        assert self.completed
        
        for ((t1, t2), state) in self.process_log:
            if state.find("Computing") >= 0:
                self.add_time("Computing", t1, t2)
            elif state.find("Waiting") >= 0:
                wait_id = int(state.split()[1])
                self.report_fifo_time(wait_id, t1, t2)
            else:
                continue

    def __repr__(self):
        return repr(self.state_log)

for log_record in log_records:

    event_text = log_record["event"]
    if event_text.find("Start task") >= 0:
        if current_task is not None:
            current_task.mark_done(log_record["time"])
        current_task_name = event_text[11:]
        current_task = TaskRecord(log_record["time"])
        tasks[current_task_name] = current_task
    elif event_text.find("Completed execution") >= 0:
        task_name = event_text[20:]
        if task_name == current_task_name:
            if current_task is not None:
                current_task.mark_done(log_record["time"])
                current_task = None
                current_task_name = None
        else:
            print >>stderr, "Ignoring completed-execution for", task_name, "because current task is", current_task_name
    elif event_text.find("Process log") >= 0:
        current_task.add_process_log(event_text[12:])
    elif event_text.find("Fetch trace") >= 0:
        current_task.add_fetch_log(event_text[12:])
    elif event_text.find("Fetch FIFO trace") >= 0:
        current_task.add_fifo_log(event_text[17:])

state_summaries = dict()

def classify_state(state):
    if state.find("Computing") >= 0:
        return "Compute-bound"
    elif state.find("buffered") >= 0 or state.find("EOF") >= 0:
        return "IO-bound (satisfied)"
    elif state.find("Waiting for FIFO to attach") >= 0 or state.find("Waiting for first request") >= 0:
        return "Waiting for infrastructure"
    elif state.find("unbuffered") >= 0:
        return "IO-bound (limited by producer rate)"
    elif state.find("Waiting for reply") >= 0:
        return "IO-bound (limited by network RTT)"
    elif state.find("Waiting for slow producer") >= 0:
        return "IO-bound (limited by producer rate [stalled])"
    elif state.find("cached") >= 0:
        return "IO-bound (limited by disk)"
    else:
        return "Unknown state"

interesting_tasks = None
if len(sys.argv) > 2:
    interesting_tasks = []
    with open(sys.argv[2], "r") as task_file:
        for task in task_file:
            interesting_tasks.append("'%s'" % task[:-1])

for (taskname, task) in tasks.items():

    if interesting_tasks is None or taskname in interesting_tasks:

        print "Processing log", taskname

        task.produce_time_series()
        for ((start_t, stop_t), state) in task.state_log:
            print (stop_t - start_t), state
            state_class = classify_state(state)
            if state_class not in state_summaries:
                state_summaries[state_class] = timedelta()
            state_summaries[state_class] += (stop_t - start_t)

    else:

        print "Ignoring uninteresting task", taskname

print "Summary:"

for state, t in state_summaries.items():
    print state, t
