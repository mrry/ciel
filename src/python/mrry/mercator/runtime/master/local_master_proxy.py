'''
Created on 15 Apr 2010

@author: dgm36
'''

class LocalMasterProxy:
    
    def __init__(self, task_pool, block_store, global_name_directory):
        self.task_pool = task_pool
        self.block_store = block_store
        self.global_name_directory = global_name_directory
    
    def publish_global_object(self, global_id, urls):
        self.global_name_directory.add_urls_for_id(global_id, urls)
        
    def spawn_tasks(self, parent_task_id, task_descriptors):
        spawn_result_ids = []
        for task in task_descriptors:
            try:
                expected_outputs = task['expected_outputs']
            except KeyError:
                try:
                    num_outputs = task['num_outputs']
                    expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                except:
                    expected_outputs = self.global_name_directory.create_global_id()
                task['expected_outputs'] = expected_outputs

            self.task_pool.add_task(task)
            spawn_result_ids.append(expected_outputs) 

        return spawn_result_ids

    def commit_task(self, task_id, commit_bindings):
        for global_id, urls in commit_bindings.items():
            self.data_store.add_urls_for_id(global_id, urls)
        return True
                           
    def failed_task(self, task_id):
        raise Exception()
        