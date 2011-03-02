#!/usr/bin/python

import math
import sys
import matplotlib.pyplot as plt

sys.stdin.readline()

min_start = None
max_end = None
n_tasks = 0

for line in sys.stdin.readlines():
    fields = line.split()
    
    start = float(fields[4])
    end = float(fields[5])
    worker = fields[11]

    min_start = start if min_start is None else min(min_start, start)
    max_end = end if max_end is None else max(max_end, end)

    n_tasks += 1

print n_tasks, max_end - min_start

