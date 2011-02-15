#!/usr/bin/python

import math
import sys
import matplotlib.pyplot as plt

sys.stdin.readline()

min_start = None
max_end = None
n_tasks = 0

splits = []

for line in sys.stdin.readlines():
    fields = line.split()
    
    start = float(fields[4])
    end = float(fields[5])
    worker = fields[11]

    if fields[1] == 'reduce':
        splits.append(end)

    min_start = start if min_start is None else min(min_start, start)
    max_end = end if max_end is None else max(max_end, end)

    n_tasks += 1

splits.sort()

prev_split = min_start
for split in splits:
    print split - prev_split
    prev_split = split



