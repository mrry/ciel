#!/usr/bin/python

import sys
import matplotlib.pyplot as plt

sys.stdin.readline()

min_start = None
max_end = None

worker_bars = {}

for line in sys.stdin.readlines():
    fields = line.split()
    
    start = float(fields[4])
    end = float(fields[5])
    worker = fields[11]

    min_start = start if min_start is None else min(min_start, start)
    max_end = end if max_end is None else max(max_end, end)

    try:
        worker_bar = worker_bars[worker]
    except KeyError:
        worker_bar = []
        worker_bars[worker] = worker_bar

    worker_bar.append((start, end - start))

fig = plt.figure()
ax = fig.add_subplot(111)

curr_worker_y = 1

for worker, bar in worker_bars.items():
    normalised_bar = [(x - min_start, t) for (x, t) in bar]

    ax.broken_barh(normalised_bar, (curr_worker_y - 0.5, 1.0), facecolors='gray', linewidth=1, edgecolor='white')
    curr_worker_y += 1

ax.set_xlim(0, max_end - min_start)
ax.set_xticks([0, max_end - min_start])
ax.set_yticks([1, 5, 10, 15, 20])
ax.set_ylim(0.5, len(worker_bars) + 0.5)
ax.set_ylabel('worker index')
ax.set_xlabel('seconds since start')
ax.grid(False)

plt.show()


#print worker_bars
#print min_start, max_end
