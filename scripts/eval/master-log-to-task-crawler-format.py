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

tasks = {}

for line in sys.stdin.readlines():
    fields = line.split()

    
    if len(fields) > 1 and fields[1] == 'TASK':
        task_id = fields[2]

        state = fields[3]
        
        worker_id = fields[4]

        timestamp = float(fields[6])

        try:
            attempt = tasks[task_id]
        except KeyError:
            attempt = TaskAttempt(task_id)
            tasks[task_id] = attempt

        attempt.worker = worker_id

        if state == 'ASSIGNED':
            attempt.start_time = timestamp
        elif state == 'COMMITTED':
            attempt.finish_time = timestamp

print 'task_id type parent created_at assigned_at committed_at duration num_children num_dependencies num_outputs final_state worker'
           
for task in tasks.values():
    print task.attempt_id, 'swi', None, None, task.start_time, task.finish_time, (task.finish_time - task.start_time) if task.finish_time is not None else None, 0, 0, 0, 'COMMITTED', task.worker
