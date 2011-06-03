import sys

if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        f.readline()

        start = None
        end = None

        for line in f.readlines():
            fields = line.split()
            
            task_created = float(fields[3])
            start = min(start, task_created) if start is not None else task_created

            task_committed = float(fields[5])
            end = max(end, task_committed) if end is not None else task_committed


    print sys.argv[1], end - start
