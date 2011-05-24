import sys
import numpy

from matplotlib import rc, use
#use('Agg')
import matplotlib.pylab as plt

#rc('font',**{'family':'sans-serif','sans-serif':['Helvetica'],'serif':['Helvetica'], 'size':8})
#rc('text', usetex=True)
#rc('figure', figsize=(3,2))
#rc('figure.subplot', left=0.2, top=0.9, bottom=0.2)
#rc('axes', linewidth=0.5)
#rc('lines', linewidth=0.5)

all_file_iterations = []
all_stage_lengths = []


# List of task-crawler files as input.
for i in sys.argv[1:]:

    with open(i) as f:

        # Discard header
        f.readline()

        current_file_iterations = []
        current_stage_lengths = []
        bytes_fetched = 0

        start_time = None
        end_time = None

        for line in f.readlines():
            
            fields = line.split()

            if (fields[7], fields[8], fields[9]) == ('0', '6', '1'):
                bytes_fetched += float(fields[12])

                if start_time is None:
                    start_time = float(fields[4])
                else:
                    start_time = min(start_time, float(fields[4]))

                if end_time is None:
                    end_time = float(fields[5])
                else:
                    end_time = max(end_time, float(fields[5]))
                    

            elif fields[7] == '101':
                current_file_iterations.append(bytes_fetched)
                current_stage_lengths.append(end_time - start_time)
                bytes_fetched = 0
                start_time = None
                end_time = None

        #print current_file_iterations

        all_file_iterations.append(current_file_iterations)
        all_stage_lengths.append(current_stage_lengths)

#transposed_all = [[x[i] for x in all_file_iterations] for i in range(len(all_file_iterations[0]))]

transposed_all = numpy.transpose(all_file_iterations)
transposed_lengths = numpy.transpose(all_stage_lengths)

averages = map(numpy.average, transposed_all)
errors = map(numpy.std, transposed_all)

average_times = map(numpy.average, transposed_lengths)
error_lengths = map(numpy.std, transposed_lengths)

plt.subplot(211)
plt.bar(range(len(averages)), averages, yerr=errors)

plt.subplot(212)
plt.bar(range(len(average_times)), average_times, yerr=error_lengths)

plt.show()



#print transposed_all

#print map(numpy.average, transposed_all)
#print map(numpy.std, transposed_all)
