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
all_nonlocal = []

# List of task-crawler files as input.
for index, i in enumerate(sys.argv[1:]):

    if index in (9, 25):
        print "skip"
        continue

    with open(i) as f:

        # Discard header
        f.readline()

        current_file_iterations = []
        current_stage_lengths = []
        current_nonlocal = []
        bytes_fetched = 0
        non_local_tasks = 0

        start_time = None
        end_time = None

        for line in f.readlines():
            
            fields = line.split()

            if (fields[7], fields[8], fields[9]) == ('0', '6', '1'):
                bytes_fetched += float(fields[12])

                if float(fields[12]) > 100000000:
                    non_local_tasks += 1

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
                current_nonlocal.append(non_local_tasks)
                bytes_fetched = 0
                non_local_tasks = 0
                start_time = None
                end_time = None

        #print current_file_iterations

        all_file_iterations.append(current_file_iterations)
        all_stage_lengths.append(current_stage_lengths)
        all_nonlocal.append(current_nonlocal)
        
        print " ".join(["%3d" % int(x) for x in current_stage_lengths])

total_lengths = map(sum, all_stage_lengths)
print total_lengths

#transposed_all = [[x[i] for x in all_file_iterations] for i in range(len(all_file_iterations[0]))]

transposed_all = numpy.transpose(all_file_iterations)
transposed_lengths = numpy.transpose(all_stage_lengths)
transposed_nonlocal = numpy.transpose(all_nonlocal)

averages = map(numpy.average, transposed_all)
errors = map(numpy.std, transposed_all)

average_times = map(numpy.average, transposed_lengths)
error_lengths = map(numpy.std, transposed_lengths)

average_nonlocal = map(numpy.average, transposed_nonlocal)
error_nonlocal = map(numpy.std, transposed_nonlocal)

plt.subplot(311)
plt.bar(range(len(averages)), averages, yerr=errors)

plt.subplot(312)
plt.bar(range(len(average_times)), average_times, yerr=error_lengths)

plt.subplot(313)
plt.bar(range(len(average_nonlocal)), average_nonlocal, yerr=error_nonlocal)


plt.show()



#print transposed_all

#print map(numpy.average, transposed_all)
#print map(numpy.std, transposed_all)
