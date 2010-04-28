'''
Created on 15 Apr 2010

@author: dgm36
'''

class LocalMasterProxy:
    
    def __init__(self, task_pool, block_store, global_name_directory, worker_pool):
        self.task_pool = task_pool
        self.block_store = block_store
        self.global_name_directory = global_name_directory
        self.worker_pool = worker_pool
    
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
        
    def get_task_descriptor_for_future(self, ref):
        task_id = self.global_name_directory.get_task_for_id()
        task = self.task_pool.get_task_by_id(task_id)
        task_descriptor = task.as_descriptor()
        task_descriptor['is_running'] = task.worker_id is not None
        if task.worker_id is not None:
            task_descriptor['worker'] = self.worker_pool.get_worker_by_id(task.worker_id).as_descriptor()
        return task_descriptor