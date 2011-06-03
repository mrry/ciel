'''
Created on May 23, 2011

@author: derek
'''
import sys
import random

def load_generator(num_workers, num_blocks, replication, alloc_strategy):
    
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
          
    max_load = max([len(x) for x in worker_stores])
            
    return [(i, len([x for x in worker_stores if len(x) == i]) ) for i in range(max_load + 1)]
            
if __name__ == '__main__':
    
    bins_random = {}
    
    for j in range(int(sys.argv[2])):
        for i, load in load_generator(100, 100, 3, sys.argv[1]):
            try:
                bins_random[i] += load
            except KeyError:
                bins_random[i] = load
        
    for i in sorted(bins_random.keys()):
        print i, bins_random[i]
        
    
    #print load_generator(100, 100, 3, 'tworandom')