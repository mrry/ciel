'''
Created on 11 Feb 2010

@author: dgm36
'''
from uuid import uuid4
from threading import Lock
import cherrypy
import collections

class ConcreteInput:
    pass
CONCRETE_INPUT = ConcreteInput()

class WorkflowBase:
    
    def __init__(self):
        self.id = str(uuid4())
        self.job_counter = 0
        self._lock = Lock()
        
    def lock(self):
        self._lock.acquire()
        
    def unlock(self):
        self._lock.release()

class Workflow:
    
    def __init__(self):
        self.id = uuid4()
        self.inputs = set()
        self.production_rules = {}
        
    def add_input(self, input_name):
        self.inputs.add(input_name)
        self.add_production_rule(input_name, CONCRETE_INPUT, [])
        
    # TODO: fix this interface so that it supports multiple outputs.
    def add_production_rule(self, output_name, procedure, inputs):
        try:
            rules = self.production_rules[output_name]
        except KeyError:
            rules = []
            self.production_rules[output_name] = rules
            
        rules.append((procedure, inputs))

    def topo_visit(self, output_name, ordering, visited_set):
        if output_name not in visited_set:
            visited_set.add(output_name)
            rules = self.production_rules[output_name]
            procedure, parents = rules[0]
            for input_name in parents:
                self.topo_visit(input_name, ordering, visited_set)
            ordering.append((output_name, procedure, parents)) 
    
    def get_ordering(self):
        visited_set = set()
        ordering = collections.deque()
        for output_name in self.production_rules.keys():
            self.topo_visit(output_name, ordering, visited_set)
            
        return ordering
        
    def print_ordering(self):
        for output_name, procedure, parents in self.get_ordering():
            print "%s = %s(%s)" % (output_name, procedure, ", ".join(parents))
            
class WorkflowExecutor:
    
    def __init__(self, workflow):            
        self.workflow = workflow
        self.concrete_data = set(workflow.inputs)
        self.task_queue = workflow.get_ordering()

    def get_next_task(self):
        if len(self.task_queue) == 0:
            raise
        output_name, procedure, parents = self.task_queue[0]
        if all([x in self.concrete_data for x in parents]):
            return self.task_queue.popleft()
        # TODO: consider whether we should look further down the list?
        return None
    
    def add_concrete_data(self, output_name):
        self.concrete_data.add(output_name)

class JobDetails:
    
    def __init__(self, workflow, job_type, inputs):
        self.id = workflow.id + "__" + workflow.job_counter 
        self.job_type = job_type
        self.inputs = inputs

class MapReduceWorkflow(WorkflowBase):
    
    def __init__(self, map_inputs, mapper_class, reducer_class, num_reducers):
        self.map_inputs = map_inputs
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.num_reducers = num_reducers
        
        self.runnable_jobs = {}
        for map_input in self.map_inputs:
            map_job = JobDetails(self, "map", [mapper_class, num_reducers, map_input])
            self.runnable_jobs[map_job.id] = map_job
        
        self.running_jobs = {}
    
        self.map_outputs = {}

        self.is_completed = False
    
    
    def get_runnable_jobs(self):
        return self.runnable_jobs
    
    def mark_job_as_running(self, job_id):
        job = self.runnable_jobs[job_id]
        del self.runnable_jobs[job_id]
        self.running_jobs[job_id] = job
        
    def mark_job_as_completed(self, job_id, outputs):
        job = self.running_jobs[job_id]
        del self.running_jobs[job_id]
    
        # Stow away the outputs in a matrix somewhere.
        self.map_outputs[job_id] = outputs
    
        if len(self.runnable_jobs) == 0 and len(self.running_jobs) == 0:
            if job.job_type == 'map':
                # Last map task completed, so schedule the reduces.
                for i in range(self.num_reduces):
                    reduce_input = []
                    for map_output in self.map_outputs.values():
                        reduce_input.append(map_output[i])
                    reduce_job = JobDetails(self, "reduce", [self.reducer_class, reduce_input])
                    self.runnable_jobs[reduce_job.id] = reduce_job
                pass
            else:
                # Last reduce task completed, so return something useful.
                cherrypy.engine.log("Workflow %s completed" % (self.id, ))
                self.is_completed = True
    
def build_mapreduce_workflow(workflow_details):
    map_inputs = list(workflow_details["map_inputs"])
    mapper_class = str(workflow_details["mapper_class"])
    reducer_class = str(workflow_details["reducer_class"])
    num_reducers = int(workflow_details["num_reducers"])



def build_dependency_graph_workflow(workflow_details):
    pass

def build_cloudscript_workflow(workflow_details):
    pass

workflow_builders = { 'mapreduce' : build_mapreduce_workflow, 
                      'dependency_graph' : build_dependency_graph_workflow, 
                      'cloudscript' : build_cloudscript_workflow }

def build_workflow(workflow_details):
    workflow_type = workflow_details['workflow_type']
    return workflow_builders[workflow_type](workflow_details)

class WorkflowCollection:
    
    def __init__(self):
        self.workflows = {}

    def get(self, workflow_id):
        return self.workflows[workflow_id]
    
    def add(self, workflow_id, workflow):
        self.workflows[workflow_id] = workflow
        
    def remove(self, workflow_id):
        del self.workflows[workflow_id]

if __name__ == '__main__':
    
    w = Workflow()
    for i in range(10):
        w.add_input("map_input_%d" % (i, ))
        w.add_production_rule("map_output_%d" % (i, ), "map", ["map_input_%d" % (i, )])
        reduce_inputs = ["map_output_%d" % (j, ) for j in range(10)]
        w.add_production_rule("reduce_output_%d" % (i, ), "reduce", reduce_inputs)
    reduce_outputs = ["reduce_output_%d" % (j, ) for j in range(10)]
    w.add_production_rule("reduce_all", "collect", reduce_outputs)

    we = WorkflowExecutor(w)

    while True:
        output_name, procedure, parents = we.get_next_task()
        we.add_concrete_data(output_name)
        if output_name == "reduce_all":
            print "DONE!"
            break
