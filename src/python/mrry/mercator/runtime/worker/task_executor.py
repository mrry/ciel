'''
Created on 13 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from mrry.mercator.cloudscript.context import SimpleContext, TaskContext,\
    LambdaFunction
from mrry.mercator.cloudscript.visitors import SWDataReference,\
    StatementExecutorVisitor, ExecutionInterruption, SWDereferenceWrapper,\
    SWFutureReference
from django.utils import simplejson
from mrry.mercator.cloudscript.interpreter.executors import StdinoutExecutor
from mrry.mercator.cloudscript import ast
import pickle
import urllib
import urllib2

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
    
    def handle_input(self, input):
        assert input['handler'] == 'swi'
        return self.handle_swi_task(input)
        
    def handle_swi_task(self, input):
        # Fetch all inputs to localhost.
        input_filenames = {}
        for (id, uris) in input['inputs'].items():
            # TODO: iterate through the URIs.
            # TODO: handle swfs URIs (http only at present).
            filename, _ = urllib.urlretrieve(uris[0])
            real_inputs[id] = SWTaskLocalDataReference(filename)
        
        # Build interpreter task and run it.
        continuation = pickle.load(open(real_inputs['_cont_']))

        # Load all dereferenced data into the continuation.
        continuation 

        # Package up the output and publish it to the callback-er
        
        pass

class ReferenceTableEntry:
    
    def __init__(self, reference):
        self.reference = reference
        self.is_dereferenced = False
    
class SWContinuation:
    
    def __init__(self, task_stmt):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = SimpleContext()
        self.reference_table = {}
        
    def create_tasklocal_reference(self, ref):
        id = self.current_local_id_index
        self.current_local_id_index += 1
        self.reference_table[id] = ReferenceTableEntry(ref)
        return SWLocalReference(id)

    def store_tasklocal_reference(self, id, ref):
        """Used when copying references to a spawned continuation."""
        self.reference_table[id] = ReferenceTableEntry(ref)
        self.current_local_id_index = max(self.current_local_id_index, id + 1)
    
    def mark_as_dereferenced(self, ref):
        self.reference_table[ref.id].is_dereferenced = True
        
    def store_dereference_value(self, id, value):
        self.reference_table[id].reference = SWDereferencedData(value) 
        
    def resolve_tasklocal_reference(self, ref):
        return self.reference_table[ref.id].reference

class SWDereferencedData:
    
    def __init__(self, value):
        self.value = value

class SWLocalReference:
    
    def __init__(self, id):
        self.id = id

class SWSpawnedTaskReference:
    
    def __init__(self, spawn_index):
        self.index = spawn_index

class SWRuntimeInterpreterTask:
    
    def __init__(self, task_id, continuation): # scheduler, task_expr, is_root=False, result_ref_id=None, result_ref_id_list=None, context=None, condvar=None):
        self.continuation_will_require = set()
        
        
        self.dereferences = set()
        
        self.task_id = task_id
        self.continuation = continuation

        self.expected_outputs = {}
        self.local_outputs = {}
        self.spawn_list = []

        self.current_local_id_index = 0
        
        # FIXME: set up result references.

    def interpret(self):
        self.continutation.context.restart()
        task_context = TaskContext(self.continuation.context, self)
        
        task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.spawn_func(x[0], x[1])))
        task_context.bind_tasklocal_identifier("spawn_list", LambdaFunction(lambda x: self.spawn_list_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
        task_context.bind_tasklocal_identifier("exec", LambdaFunction(lambda x: self.exec_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("ref", LambdaFunction(lambda x: self.make_reference(x)))
        visitor = StatementExecutorVisitor(task_context)
        
        try:
            self.result = visitor.visit(self.continuation.task_stmt, self.continuation.stack, 0)

            # FIXME: handle completion event by building task result structure.
            self.task_output = None
            
        except ExecutionInterruption as exi:
            # FIXME: handle continuation event by building task result structure.
            self.task_output = None
            
        except Exception as e:
            # FIXME: handle exception event by building task result structure.
            self.task_output = None

        return { 'task_id' : self.task_id,
                 'expected_outputs' : self.expected_outputs,
                 'local_outptus' : self.local_outputs,
                 'spawn_list' : self.spawn_list }

    def spawn_func(self, spawn_expr, args):
        # Create new continuation for the spawned function.
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        spawned_continuation = SWContinuation(spawned_task_stmt)
        # FIXME: consider spawning a tasklocal identifier like exec or spawn....
        
        # Create swfs URI for continuation object.
        # FIXME: write to datanode component.
        # TODO: handle in memory if possible.
        
        # Record any references that might be necessary inputs to the spawned function.
        spawned_task_inputs = set()
        
        def handle_value(value):
            if isinstance(value, SWDereferenceWrapper):
                # TODO: decide what to do with obtained dereference objects.
                #       Probably should mark them as task inputs.
                spawned_task_inputs.add(value.ref.id)
            elif isinstance(value, SWLocalReference):
                spawned_continuation.store_tasklocal_reference(value.id, self.continuation.resolve_tasklocal_reference(value))
        
        map(handle_value, spawn_expr.captured_bindings.values())
        map_over_sw_data_structure(handle_value, args)

        # Match up the output with a new tasklocal reference.
        ret = self.continuation.create_tasklocal_reference(SWSpawnedTaskReference(len(self.spawn_list)))
        
        # Append the new task definition to the spawn list.
        task_descriptor = { 'handler' : 'swi',
                            'inputs' : list(spawned_task_inputs),
                            'results' : [ret.id] }
        
        self.spawn_list.append(task_descriptor)

        # Return local reference to the interpreter.
        return ret
        
    def spawn_list_func(self, spawn_expr, args, num_outputs):
        # Create new continuation for the spawned function.
        # FIXME: consider spawning a tasklocal identifier like exec or spawn....
        
        # Create swfs URI for continuation object.
        # FIXME: handle in memory if possible.
        
        # Record any references that might be necessary inputs to the spawned function.
        # FIXME: add a means of walking the spawn context.
        
        # Match up the outputs with new tasklocal references.

        # Append the new task definition to the spawn list.
        pass

    def exec_func(self, executor_name, args, num_outputs):
        executor_class_map = {'stdinout' : StdinoutExecutor}
        try:
            # FIXME: may need to pass the continuation through because will need to deref the args.
            executor = executor_class_map[executor_name](args, num_outputs)
        except KeyError:
            raise "No such executor: %s" % (executor_name, )
            
        executor.execute()
        
        # FIXME: process executor.output_urls into useful URLs for the reference.
        return map(lambda x: self.continuation.create_tasklocal_reference(SWDataReference([x])), executor.output_urls)    

    def make_reference(self, urls):
        # TODO: should we add this to local_outputs?
        return self.continuation.create_tasklocal_reference(SWDataReference(urls))

    def lazy_dereference(self, ref):
        self.continuation_will_require.add(ref.id)
        self.continuation.mark_as_dereferenced(ref)
        return SWDereferenceWrapper(ref)
        
    def eager_dereference(self, ref):
        real_ref = self.continuation.resolve_tasklocal_ref(ref)
        if isinstance(real_ref, SWDereferencedData):
            return real_ref.value
        else:
            # TODO: consider loading this immediately if it is local.
            self.continuation_will_require.add(ref.id)
            self.continuation.mark_as_dereferenced(ref)
            raise ExecutionInterruption()
        
def map_over_sw_list(f, l):
    for item in list:
        map_over_sw_data_structure(f, item)
        
def map_over_sw_dict(f, d):
    for key, value in d.items():
        map_over_sw_data_structure(f, key)
        map_over_sw_data_structure(f, value)
        
def map_over_sw_data_structure(f, structure):
    if structure is list:
        map_over_sw_list(f, structure)
    elif structure is dict:
        map_over_sw_dict(f, structure)
    else:
        f(structure)