#!/usr/bin/python

from matplotlib import rc, use
use('Agg')
import math
import sys
import matplotlib.pylab as plt

rc('font',**{'family':'sans-serif','sans-serif':['Helvetica'],'serif':['Helvetica'], 'size':8})
rc('text', usetex=True)
rc('figure', figsize=(3,2))
rc('figure.subplot', left=0.2, top=0.9, bottom=0.2)
rc('axes', linewidth=0.5)
rc('lines', linewidth=0.5)

xs = []
ys = []
durations = []

for infile in sys.argv[1:]:
    with open(infile) as first:

        first.readline()
    
        min_start = None
        max_end = None
    
        worker_bars = {}
    
        events = []
    
        for line in first.readlines():
            fields = line.split()

            if fields[5] == 'None':
                continue
        
            start = float(fields[4])
            end = float(fields[5])
            worker = fields[11]

            events.append((start, 1, worker))
            events.append((end, -1, worker))

            min_start = start if min_start is None else min(min_start, start)
            max_end = end if max_end is None else max(max_end, end)

        events.sort()

        xseries1 = []
        yseries1 = []
        
        active_workers = {}

        for t, delta, w in events:
            xseries1.append(t - min_start)
            yseries1.append(len(active_workers))
            try:
                active_workers[w] += delta
                if active_workers[w] == 0:
                    del active_workers[w]
            except KeyError:
                active_workers[w] = delta
            xseries1.append(t - min_start)
            yseries1.append(len(active_workers))

        duration1 = max_end - min_start

    xs.append(xseries1)
    ys.append(yseries1)
    durations.append(duration1)

#fig = plt.figure()

#plt.subplots_adjust(wspace=0.2)

plt.figure()
plt.ylabel('Number of chunks')


i = 1
for (xseries1, yseries1, col, chunks) in zip(xs, ys, ['red', 'red', 'blue', 'red', 'red'], [10, 20, 30, 40, 50]):
    ax = plt.subplot(len(xs), 1, i, frame_on=False)
    plt.plot(xseries1, yseries1, color=col)
    plt.xlim(0, math.ceil(max(durations)))
    plt.ylabel(r'$%d \times %d$' % (chunks, chunks), rotation='horizontal')
    plt.xticks([])
    plt.yticks([])
    plt.axvline(0, 0, 20, color='k')
    plt.axhline(0, 0, color='k')
    #lbl = plt.ylabel(sys.argv[i][-6:-4])
    #lbl.set_rotation(90)
    plt.ylim(0, 21)
    i += 1

print ax.axis()
plt.xticks([0, min(durations), max(durations)], ['0', str(int(math.ceil(min(durations)))), str(int(math.ceil(max(durations))))])
plt.xlabel('Time [sec]')




#ax.tick_params(top='off', right='off')

#ax.spines['top'].set_color('none')
#ax.spines['right'].set_color('none')

# ax = plt.subplot(212, frame_on=False)
# plt.plot(xseries2, yseries2, 'r-')
# plt.xlim(0, math.ceil(max(duration1, duration2)))
# plt.ylim(0, 21)
# ax.tick_params(top='off', right='off')
# plt.xticks([0, math.ceil(duration1), math.ceil(duration2)])
# plt.yticks([0, 20], ['0', '20'])
# #ax.spines['top'].set_color('none')
# #ax.spines['right'].set_color('none')

# plt.ylabel('Hadoop')

# plt.xticks((0, math.ceil(min(duration1, duration2)), math.ceil(max(duration1, duration2))), ('0', str(int(math.ceil(min(duration1, duration2)))), str(int(math.ceil(max(duration1, duration2))))))


plt.savefig('smithwaterman-util.pdf', format='pdf')
