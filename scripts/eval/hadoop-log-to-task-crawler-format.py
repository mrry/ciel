#!/usr/bin/python

import sys
import matplotlib.pyplot as plt

class TaskAttempt:
    def __init__(self, attempt_id):
        self.attempt_id = attempt_id
        self.start_time = None
        self.finish_time = None
        self.worker = None

    def __repr__(self):
        return 'TaskAttempt(%s, %f, %f)' % (self.attempt_id, self.start_time, self.finish_time)

map_tasks = {}
reduce_tasks = {}

for line in sys.stdin.readlines():
    fields = line.split()
 
    if fields[0] == 'MapAttempt':
        attempt_id = fields[3][len('TASK_ATTEMPT_ID="'):-1]

        try:
            attempt = map_tasks[attempt_id]
        except KeyError:
            attempt = TaskAttempt(attempt_id)
            map_tasks[attempt_id] = attempt

        if fields[4].startswith('START_TIME='):
            attempt.start_time = int(fields[4][len('START_TIME="'):-1]) / 1000
            attempt.worker = fields[5][len('TRACKER_NAME="'):-1]
        elif fields[5].startswith('FINISH_TIME='):
            attempt.finish_time = int(fields[5][len('FINISH_TIME="'):-1]) / 1000
            if fields[4] == 'TASK_STATUS="KILLED"':
                del map_tasks[attempt_id]

    elif fields[0] == 'ReduceAttempt':
        attempt_id = fields[3][len('TASK_ATTEMPT_ID="'):-1]

        try:
            attempt = reduce_tasks[attempt_id]
        except KeyError:
            attempt = TaskAttempt(attempt_id)
            reduce_tasks[attempt_id] = attempt

        if fields[4].startswith('START_TIME='):
            #attempt.start_time = int(fields[4][len('START_TIME="'):-1]) / 1000
            attempt.worker = fields[5][len('TRACKER_NAME="'):-1]
        elif fields[7].startswith('FINISH_TIME='):
            attempt.start_time = int(fields[5][len('SHUFFLE_FINISHED="'):-1]) / 1000
            attempt.finish_time = int(fields[7][len('FINISH_TIME="'):-1]) / 1000
            

print 'task_id type parent created_at assigned_at committed_at duration num_children num_dependencies num_outputs final_state worker'
           
num_maps = len(map_tasks)
num_reduces = len(reduce_tasks)

for attempt in map_tasks.values():
    print attempt.attempt_id, 'map', None, None, attempt.start_time, attempt.finish_time, attempt.finish_time - attempt.start_time, 0, 1, num_reduces, 'COMMITTED', attempt.worker

for attempt in reduce_tasks.values():
    print attempt.attempt_id, 'reduce', None, None, attempt.start_time, attempt.finish_time, attempt.finish_time - attempt.start_time, 0, num_maps, 1, 'COMMITTED', attempt.worker

    #start = float(fields[4])
    #end = float(fields[5])
    #worker = fields[11]

    #min_start = start if min_start is None else min(min_start, start)
    #max_end = end if max_end is None else max(max_end, end)

