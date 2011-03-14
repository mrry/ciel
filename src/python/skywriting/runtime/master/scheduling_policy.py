# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
from shared.references import SW2_SweetheartReference, SW2_ConcreteReference,\
    SW2_StreamReference
import random


class SchedulingPolicy:
    
    def __init__(self):
        pass
    
    def select_worker_for_task(self, task, worker_pool):
        """Returns a list of workers on which to run the given task."""
        raise Exception("Subclass must implement this")
    
class RandomSchedulingPolicy(SchedulingPolicy):
    
    def __init__(self):
        pass
    
    def select_worker_for_task(self, task, worker_pool):
        return (worker_pool.get_random_worker(), [])
    
class WeightedRandomSchedulingPolicy(SchedulingPolicy):
    
    def __init__(self):
        pass
    
    def select_worker_for_task(self, task, worker_pool):
        return worker_pool.get_random_worker_with_capacity_weight(task.scheduling_class)
    
class TwoRandomChoiceSchedulingPolicy(SchedulingPolicy):
    
    def __init__(self):
        pass
    
    def select_worker_for_task(self, task, worker_pool):
        worker1 = worker_pool.get_random_worker()
        worker2 = worker_pool.get_random_worker()
        cost1 = task.job.guess_task_cost_on_worker(task, worker1)
        cost2 = task.job.guess_task_cost_on_worker(task, worker2)
        if cost1 < cost2:
            return (worker1, [])
        else:
            return (worker2, [])

class LocalitySchedulingPolicy(SchedulingPolicy):
    
    def __init__(self, sweetheart_factor=1000, equally_local_margin=0.9, stream_source_bytes_equivalent=10000000, min_saving_threshold=1048576):
        self.sweetheart_factor = sweetheart_factor
        self.equally_local_margin = equally_local_margin
        self.stream_source_bytes_equivalent = stream_source_bytes_equivalent 
        self.min_saving_threshold = min_saving_threshold
    
    def select_worker_for_task(self, task, worker_pool):
        netlocs = {}
        for input in task.inputs.values():
            
            if isinstance(input, SW2_SweetheartReference) and input.size_hint is not None:
                # Sweetheart references get a boosted benefit for the sweetheart, and unboosted benefit for all other netlocs.
                try:
                    current_saving_for_netloc = netlocs[input.sweetheart_netloc]
                except KeyError:
                    current_saving_for_netloc = 0
                netlocs[input.sweetheart_netloc] = current_saving_for_netloc + self.sweetheart_factor * input.size_hint
                
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
                    
            elif isinstance(input, SW2_ConcreteReference) and input.size_hint is not None:
                # Concrete references get an unboosted benefit for all netlocs.
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + input.size_hint
                    
            elif isinstance(input, SW2_StreamReference):
                # Stream references get a heuristically-chosen benefit for stream sources.
                for netloc in input.location_hints:
                    try:
                        current_saving_for_netloc = netlocs[netloc]
                    except KeyError:
                        current_saving_for_netloc = 0
                    netlocs[netloc] = current_saving_for_netloc + self.stream_source_bytes_equivalent
                    
        ranked_netlocs = [(saving, netloc) for (netloc, saving) in netlocs.items()]
        filtered_ranked_netlocs = filter(lambda (saving, netloc) : worker_pool.get_worker_at_netloc(netloc) is not None and saving > self.min_saving_threshold, ranked_netlocs)
        filtered_ranked_netlocs.sort()
        
        if len(filtered_ranked_netlocs) == 0:
            # If we have no preference for any worker, use the power of two random choices. [Azar et al. STOC 1994]
            worker1 = worker_pool.get_random_worker_with_capacity_weight(task.scheduling_class)
            return worker1, []
        elif len(filtered_ranked_netlocs) == 1:
            return worker_pool.get_worker_at_netloc(filtered_ranked_netlocs[0][1]), []
        
        elif filtered_ranked_netlocs[0][0] * self.equally_local_margin > filtered_ranked_netlocs[1][0]:
            # Many potential netlocs, but one clear best.
            return worker_pool.get_worker_at_netloc(filtered_ranked_netlocs[0][1]), []
        
        else:
            # Get all the equally-good netlocs, but in a random order.
            i = 2
            while i < len(filtered_ranked_netlocs) and (filtered_ranked_netlocs[0][0] * self.equally_local_margin) > filtered_ranked_netlocs[1][0]:
                i += 1
            
            ret = [worker_pool.get_worker_at_netloc(x[1]) for x in filtered_ranked_netlocs[0:i]]
            random.shuffle(ret)
            return ret[0], ret[1:]
        
SCHEDULING_POLICIES = {'random' : RandomSchedulingPolicy,
                       'tworandom' : TwoRandomChoiceSchedulingPolicy,
                       'locality' : LocalitySchedulingPolicy}

def get_scheduling_policy(policy_name, *args, **kwargs):
    if policy_name is None:
        return LocalitySchedulingPolicy()
    else:
        return SCHEDULING_POLICIES[policy_name](*args, **kwargs)