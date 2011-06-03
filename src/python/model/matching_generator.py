import sys
import random
import tempfile
import subprocess

def matching_generator(num_blocks, num_workers, replication, alloc_strategy, sched_strategy):
    
    # 1. Allocate blocks to the workers, as if loading them into the system.
    blocks = range(num_blocks)
    
    worker_stores = [set() for _ in range(num_workers)]
    block_workers = [set() for _ in range(num_blocks)]
        
    for block in blocks:
        
        if alloc_strategy == 'random':
            
            target_stores = random.sample(range(num_workers), replication)
        
            for store in target_stores:
                worker_stores[store].add(block)
                block_workers[block].add(store)
                
        elif alloc_strategy == 'tworandom':
            
            target_set = set()
            while len(target_set) < replication:
                x, y = random.sample(range(num_workers), 2)
                if len(worker_stores[x]) < len(worker_stores[y]):
                    if x not in target_set:
                        target_set.add(x)
                        worker_stores[x].add(block)
                        block_workers[block].add(x)
                else:
                    if y not in target_set:
                        target_set.add(y)
                        worker_stores[y].add(block)
                        block_workers[block].add(y)
        else:
            print >>sys.stderr, 'Unknown allocation strategy: %s' % alloc_strategy
            sys.exit(-1)
    
    if sched_strategy == 'quincy':
    
        # 2. Generate a graph for CS2
        with tempfile.NamedTemporaryFile("w", delete=False) as graphfile:
            
            # One node per block, one node per worker, one source, one sink, and one non-local sink.
            num_nodes = num_blocks + num_workers + 3
            
            # One edge source->each block, replication edges each block->workers, one edge each block->non-local sink, one edge non-local-sink->sink
            num_edges = num_blocks + num_blocks * replication + num_workers + num_blocks + 1
            
            # Define problem.
            print >>graphfile, "p min %d %d" % (num_nodes, num_edges)
            
            # Define source and sink.
            source_id = 0
            sink_id = num_nodes
            print >>graphfile, "n 0 %d" % (num_blocks)
            print >>graphfile, "n %d -%d" % (sink_id, num_blocks)
            
            non_local_sink_id = num_nodes - 1
            
            def block_index_to_id(index):
                return index + 1
            
            def worker_index_to_id(index):
                return index + num_blocks + 1
            
            # Define source -> block edges. (Mandatory flow of 1, no cost)
            for i in range(num_blocks):
                print >>graphfile, "a 0 %d 1 1 0" % (block_index_to_id(i))
                
            # Define block -> worker edges. (Flow of 0 or 1, no cost)
            for i, js in enumerate(block_workers):
                for j in js: 
                    print >>graphfile, "a %d %d 0 1 0" % (block_index_to_id(i), worker_index_to_id(j))
                # Define block -> non-local-sink edge (Flow of 0 or 1, cost 1).
                print >>graphfile, "a %d %d 0 1 1" % (block_index_to_id(i), non_local_sink_id)
            
            # Define worker -> sink edges. (Flow of 0 or 1, no cost)
            for i in range(num_workers):
                print >>graphfile, "a %d %d 0 1 0" % (worker_index_to_id(i), sink_id)
                
            # Define non-local-sink to sink edge.
            print >>graphfile, "a %d %d 0 %d 0" % (non_local_sink_id, sink_id, num_blocks)
        
        with open(graphfile.name, "r") as cs2in:
            with open('/dev/null', "r") as devnull:
                p = subprocess.Popen(["/Users/derek/tmp/cs2-4.6/cs2.exe"], stdin=cs2in, stderr=devnull, stdout=subprocess.PIPE)
        
        for line in p.stdout.readlines():
            if 'cost' in line:
                cost = int(line.split()[4])

    elif sched_strategy == 'greedy':
        
        task_list = range(num_blocks)
        random.shuffle(task_list)
        idle_workers = set(range(num_workers))
    
        cost = 0
        
        for task in task_list:
            
            assigned = False
            
            if not assigned:
                local_worker_list = list(block_workers[task])
                random.shuffle(local_worker_list)
                for worker in local_worker_list:
                    if worker in idle_workers:
                        assigned = True
                        idle_workers.remove(worker)
                        break
                
            if not assigned:
                # Allocate to a randomly-chosen idle worker.
                worker = random.choice(list(idle_workers))
                idle_workers.remove(worker)
                cost += 1

    return cost

if __name__ == '__main__':
    
    num_blocks = int(sys.argv[1])
    num_workers = int(sys.argv[2])
    replication = int(sys.argv[3])
    repeat = int(sys.argv[4])
    
    for alloc_strategy in 'tworandom',:
    
        for sched_strategy in 'greedy',:
    
            bins = {}
    
            for i in range(repeat):
                cost = matching_generator(num_blocks, num_workers, replication, alloc_strategy, sched_strategy)
                try:
                    bins[cost] += 1
                except KeyError:
                    bins[cost] = 1
                    
                if i + 1 in [10, 100, 1000, 10000, 100000, 200000, 300000, 400000, 500000, 750000, 1000000, 10000000, 100000000, 1000000000]:
                    with open("%s-%s-%d-%d-%d-x%d.txt" % (alloc_strategy, sched_strategy, num_blocks, num_workers, replication, i + 1), "w") as out:
                        sorted_bins = sorted(bins.items())
                        print alloc_strategy, sched_strategy, sorted_bins
                        for (k, v) in sorted_bins:
                            print >>out, k, v
            
            with open("%s-%s-%d-%d-%d-x%d.txt" % (alloc_strategy, sched_strategy, num_blocks, num_workers, replication, repeat), "w") as out:
                sorted_bins = sorted(bins.items())
                print alloc_strategy, sched_strategy, sorted_bins
                for (k, v) in sorted_bins:
                    print >>out, k, v