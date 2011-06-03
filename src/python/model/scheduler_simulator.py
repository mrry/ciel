'''
Created on May 23, 2011

@author: derek
'''
import sys
import random

def scheduler_simulator(num_workers, num_blocks, replication, iterations, alloc_strategy, publish_strategy):
    
    # 1. Allocate blocks to the workers, as if loading them into the system.
    blocks = range(num_blocks)
    
    worker_stores = [set() for _ in range(num_workers)]
    block_workers = [set() for _ in range(num_blocks)]
    block_sweethearts = [None for _ in range(num_blocks)]
    
    results = []
    
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
    
    # 2. Simulate schedule allocation.
    total_blocks = [num_blocks * replication]
    
    for i in range(iterations):
        task_list = range(num_blocks)
        random.shuffle(task_list)
        idle_workers = set(range(num_workers))
    
        non_local_tasks = 0
        
        
        for task in task_list:
            
            assigned = False
            
            if block_sweethearts[task] in idle_workers:
                assigned = True
                worker = block_sweethearts[task]
                idle_workers.remove(worker)
                
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
                
            if task not in worker_stores[worker]:
                non_local_tasks += 1
                worker_stores[worker].add(task)
                
            if publish_strategy == 'publish':
                block_workers[task].add(worker)
            elif publish_strategy == 'sweetheart':
                block_workers[task].add(worker)
                block_sweethearts[task] = worker
        
        results.append(non_local_tasks)
        total_blocks.append(total_blocks[-1] + non_local_tasks)
    
    return results, total_blocks
    
if __name__ == '__main__':
    
    num_workers = int(sys.argv[1])
    num_blocks = int(sys.argv[2])
    replication = int(sys.argv[3])
    iterations = int(sys.argv[4])
    repeats = int(sys.argv[5])
    
    results = [0 for i in range(iterations)]
    storage = [0 for i in range(iterations + 1)]
    
    for i in range(repeats):
        trial, total_blocks = scheduler_simulator(num_workers, num_blocks, replication, iterations, 'tworandom', None)
        results = [x + y for x, y in zip(results, trial)]
        storage = [x + y for x, y in zip(storage, total_blocks)]
        
    print results
    print [x / float(num_blocks*replication*repeats) for x in storage]
