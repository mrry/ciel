#!/usr/bin/python

import math
import sys
import matplotlib.pyplot as plt

sys.stdin.readline()

min_start = None
max_end = None

worker_bars = {}

events = []

for line in sys.stdin.readlines():
    fields = line.split()
    
    try:
        start = float(fields[4])
        end = float(fields[5])
        worker = fields[11]
    except:
        continue


    events.append((start, 1, worker))
    events.append((end, -1, worker))

    min_start = start if min_start is None else min(min_start, start)
    max_end = end if max_end is None else max(max_end, end)

events.sort()

xseries = []
yseries = []
active_workers = {}

for t, delta, w in events:
    xseries.append(t - min_start)
    yseries.append(len(active_workers))
    try:
        active_workers[w] += delta
        if active_workers[w] == 0:
            del active_workers[w]
    except KeyError:
        active_workers[w] = delta
    xseries.append(t - min_start)
    yseries.append(len(active_workers))

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(xseries, yseries)
ax.set_xlim(0, math.ceil(max_end - min_start))
ax.set_ylim(0, 21)

ax.set_ylabel('active workers')
ax.set_xlabel('seconds since start')
ax.set_xticks([0, math.ceil(max_end - min_start)])

# curr_worker_y = 1

# for worker, bar in worker_bars.items():
#     normalised_bar = [(x - min_start, t) for (x, t) in bar]

#     ax.broken_barh(normalised_bar, (curr_worker_y - 0.5, 1.0), facecolors='gray', linewidth=1, edgecolor='white')
#     curr_worker_y += 1

# ax.set_xlim(0, max_end - min_start)
# ax.set_xticks([0, max_end - min_start])
# ax.set_yticks([1, 5, 10, 15, 20])
# ax.set_ylim(0.5, len(worker_bars) + 0.5)
# ax.set_ylabel('worker index')
# ax.set_xlabel('seconds since start')
# ax.grid(False)

plt.show()

print 'Total duration', max_end - min_start

x_old = 0.0
y_old = 0.0
total = 0.0
for (x, y) in zip(xseries, yseries):
    total = total + (x - x_old) * y_old
    x_old = x
    y_old = y

print 'Total utilisation', total / 20.0
print 'Utilisation%', (total / 20.0) / (max_end - min_start)

#print worker_bars
#print min_start, max_end
