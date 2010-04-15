'''
Created on 13 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from mrry.mercator.cloudscript.context import SimpleContext, TaskContext,\
    LambdaFunction, all_leaf_values
from mrry.mercator.cloudscript.visitors import SWDataReference,\
    StatementExecutorVisitor, ExecutionInterruption, SWDereferenceWrapper
from mrry.mercator.cloudscript.interpreter.executors import StdinoutExecutor
from mrry.mercator.cloudscript import ast

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, block_store, master_proxy, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
        self.block_store = block_store
        self.master_proxy = master_proxy
    
    def handle_input(self, input):
        print 'Handling a task!'
        assert input['handler'] == 'swi'
        return self.handle_swi_task(input)
        
    def handle_swi_task(self, task_descriptor):
        print 'Handling an SWI task!'
        try:     
            task_id = task_descriptor['task_id']
        except KeyError:
            return
                
        try:
            interpreter = SWRuntimeInterpreterTask(task_descriptor, self.block_store)
            interpreter.fetch_inputs()
            interpreter.interpret()
            interpreter.spawn_all()
            interpreter.commit_task()
        except:
            self.master_proxy.failed_task(task_id)

class ReferenceTableEntry:
    
    def __init__(self, reference):
        self.reference = reference
        self.is_dereferenced = False
        self.is_execd = False
        self.is_returned = False
        
class SpawnListEntry:
    
    def __init__(self, task_descriptor, continuation):
        self.task_descriptor = task_descriptor
        self.continuation = continuation
    
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
    
    # The following methods capture why we might have blocked on something,
    # for appropriate handling on task loading.
    def mark_as_dereferenced(self, ref):
        self.reference_table[ref.id].is_dereferenced = True
    def is_marked_as_dereferenced(self, id):
        return self.reference_table[id].is_dereferenced
    def mark_as_execd(self, id):
        self.reference_table[id].is_execd = True
    def is_marked_as_execd(self, ref):
        return self.reference_table[ref.id].is_execd
    def mark_as_returned(self, id):
        self.reference_table[id].is_returned = True
    def is_marked_as_returned(self, ref):
        return self.reference_table[ref.id].is_returned
        
    def rewrite_reference(self, id, real_ref):
        self.reference_table[id].reference = real_ref
        
    def resolve_tasklocal_reference(self, ref):
        return self.reference_table[ref.id].reference

class SWLocalReference:
    """
    A primitive object used in the interpreter, and returned from functions like
    ref() and spawn(). Contains an index into the continuation's reference table,
    which identifies the real reference object.
    """
    
    def __init__(self, index):
        self.index = index

class SWLocalFutureReference:
    """
    Used as a placeholder reference for the results of spawned tasks. Refers to the
    output of a particular task in the spawn list. If that task has multiple outputs,
    refers to a particular result.
    
    This must be rewritten before the continuation is spawned. However, it may be used
    as the argument to another spawned task.
    """
    
    def __init__(self, spawn_list_index, result_index=0):
        self.spawn_list_index = spawn_list_index
        self.result_index = result_index
        
    def as_tuple(self):
        if self.result_index is None:
            return ('lfut', self.spawn_list_index)
        else:
            return ('lfut', self.spawn_list_index, self.result_index)

class SWURLReference:
    """
    A reference to one or more URLs representing the same data.
    """
    
    def __init__(self, urls):
        self.urls = urls
        
    def as_tuple(self):
        return ('urls', self.urls)

class SWGlobalFutureReference:
    """
    Used as a reference to a task that hasn't completed yet. The identifier is in a
    system-global namespace, and may be passed to other tasks or returned from
    tasks.
    
    SWLocalFutureReferences must be rewritten to be SWGlobalFutureReference objects.
    """

    def __init__(self, id):
        self.id = id
        
    def as_tuple(self):
        return ('gfut', self.id)

class SWLocalDataFile:
    """
    Used when a reference is used as a file input (and hence should
    not be brought into the environment.
    """
    
    def __init__(self, filename):
        self.filename = filename
        
    def as_tuple(self):
        return ('lfile', self.filename)

class SWDataValue:
    """
    Used to store data that has been dereferenced and loaded into the environment.
    """
    
    def __init__(self, value):
        self.value = value
        
    def as_tuple(self):
        return ('val', self.value)

def build_reference_from_tuple(reference_tuple):
    ref_type = reference_tuple[0]
    if ref_type == 'urls':
        return SWURLReference(reference_tuple[1])
    elif ref_type == 'lfut':
        if len(reference_tuple) == 3:
            result_index = reference_tuple[2]
        else:
            result_index = None
        return SWLocalFutureReference(reference_tuple[1], result_index)
    elif ref_type == 'gfut':
        return SWGlobalFutureReference(reference_tuple[1])
    elif ref_type == 'lfile':
        return SWLocalDataFile(reference_tuple[2])
    elif ref_type == 'val':
        return SWDataValue(reference_tuple[2])
    
class SWRuntimeInterpreterTask:
    
    def __init__(self, task_descriptor): # scheduler, task_expr, is_root=False, result_ref_id=None, result_ref_id_list=None, context=None, condvar=None):
        self.task_id = task_descriptor['task_id']
        self.expected_outputs = task_descriptor['expected_outputs']
        self.inputs = task_descriptor['inputs']

        self.continuation_will_require = set()
        self.spawn_list = []

    def fetch_inputs(self, block_store):
        continuation_ref = None
        parsed_inputs = {}
        
        for local_id, ref_tuple in self.inputs.items():
            ref = build_reference_from_tuple(ref_tuple)
            if local_id == '_cont':
                continuation_ref = ref
            else:
                parsed_inputs[local_id] = ref
        
        assert isinstance(continuation_ref, SWURLReference)
        self.continuation = block_store.retrieve_object_by_url(continuation_ref.urls[0])
        
        for local_id, ref in parsed_inputs.items():
            if self.continuation.is_marked_as_dereferenced(local_id):
                if isinstance(ref, SWDataValue):
                    self.continuation.rewrite_reference(local_id, ref)
                else:
                    assert isinstance(ref, SWURLReference)
                    value = block_store.retrieve_object_by_url(ref.urls[0])
                    self.continuation.rewrite_reference(local_id, SWDataValue(value))
            elif self.continuation.is_marked_as_execd(local_id):
                if isinstance(ref, SWDataValue):
                    url = block_store.store_object(ref.value)
                    filename = block_store.retrieve_filename_by_url(url)
                else:
                    assert isinstance(ref, SWURLReference)
                    filename = block_store.retrieve_filename_by_url(ref.urls[0])
                self.continuation.rewrite_reference(local_id, SWLocalDataFile(filename))
            else:
                assert False

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
            
        except ExecutionInterruption:
            # Need to add a continuation task to the spawn list.
            cont_deps = {}
            for index in self.continuation_will_require:
                cont_deps[index] = self.continuation.resolve_tasklocal_reference(index).as_tuple()
            cont_task_descriptor = {'handler': 'swi',
                                    'inputs': cont_deps, # _cont will be added at spawn time.
                                    'expected_outputs': self.expected_outputs}
            self.spawn_list.append(SpawnListEntry(cont_task_descriptor, self.continuation))
            return
            
        except Exception as e:
            raise

    def spawn_all(self, block_store, master_proxy):
        current_batch = []
        
        spawn_task_result_global_ids = []
        
        current_index = 0
        while current_index < len(self.spawn_list):
            
            must_wait = False
            current_cont = self.spawn_list[current_index].continuation
            
            for index, ref_table_entry in enumerate(current_cont.reference_table):
                if isinstance(ref_table_entry.real_ref, SWLocalFutureReference):
                    # if unavailable in the local lookup table (from previous spawn batches), must wait;
                    # else rewrite the reference.
                    spawn_list_index = ref_table_entry.real_ref.spawn_list_index
                    if spawn_list_index >= len(spawn_task_result_global_ids):
                        must_wait = True
                        break
                    else:
                        current_cont.rewrite_reference(index, SWGlobalFutureReference(spawn_task_result_global_ids[spawn_list_index]))
                
            if must_wait:
                
                # Fire off the current batch.
                batch_result_ids = master_proxy.spawn_tasks(self.task_id, current_batch)
                
                # Update a local structure containing all of the spawn/global ids so far.
                spawn_task_result_global_ids.extend(batch_result_ids)
                
                # Iterate again on the same index.
                continue
                
            else:
                
                # Store the continuation and add it to the task descriptor.
                cont_url = block_store.store_object(current_cont)
                self.spawn_list[current_index].task_descriptor['input']['_cont'] = SWURLReference([cont_url])
            
                # Current task is now ready to be spawned.
                current_batch.append(self.spawn_list[current_index].task_descriptor)
                current_index += 1
            
        if len(current_batch > 0):
            
            # Fire off the current batch.
            master_proxy.spawn_tasks(self.task_id, current_batch)
        
    def commit_result(self, block_store, master_proxy):
        if self.result is None:
            master_proxy.commit(self.task_id, {})
            return
        
        commit_bindings = {}
        
        if self.result is list and self.expected_outputs is list:
            assert len(self.result) >= len(self.expected_outputs)
            for i, output in enumerate(self.expected_outputs):
                # TODO: handle the case where we return a spawned reference (tail-recursion).
                #       ...could make this happen in visit_Return with an "is_returned" Ref Table Entry.
                assert isinstance(self.result[i], SWLocalReference)
                commit_bindings[output] = self.continuation.resolve_tasklocal_reference(self.result[i]).urls
            
        elif self.expected_outputs is not list:
            result_url = block_store.store_object(self.result)
            commit_bindings[self.expected_outputs] = [result_url]
        
        else:
            assert False
            
        master_proxy.commit(self.task_id, commit_bindings)

    def build_spawn_continuation(self, spawn_expr, args):
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        cont = SWContinuation(spawned_task_stmt)
        
        # Now need to build the reference table for the spawned task.
        local_reference_indices = set()
        
        # Local references in the arguments.
        for leaf in filter(lambda x: isinstance(x, SWLocalReference), all_leaf_values(args)):
            local_reference_indices.add(leaf.index)
            
        # Local references captured in the lambda/function.
        for leaf in filter(lambda x: isinstance(x, SWLocalReference), all_leaf_values(spawn_expr.captured_bindings)):
            local_reference_indices.add(leaf.index)

        if len(local_reference_indices) > 0:
            cont.current_local_id_index = max(local_reference_indices) + 1

        # Actually build the new reference table.
        # TODO: This would be better if we compressed the table, but might take a while.
        #       So let's assume that we won't run out of indices in a normal run :).
        for index in local_reference_indices:
            cont.reference_table[index] = self.continuation.reference_table[index]
        
        return cont

    def spawn_func(self, spawn_expr, args):
        # Create new continuation for the spawned function.
        spawned_continuation = self.build_spawn_continuation(spawn_expr, args)
        
        # Match up the output with a new tasklocal reference.
        ret = self.continuation.create_tasklocal_reference(SWLocalFutureReference(len(self.spawn_list)))
        
        # Append the new task definition to the spawn list.
        task_descriptor = {'handler': 'swi',
                           'inputs': {}, # _cont will be added later
                          }
        
        # TODO: we could visit the spawn expression and try to guess what requirements
        #       and executors we need in here. 
        # TODO: should probably look at dereference wrapper objects in the spawn context
        #       and ship them as inputs.
        
        self.spawn_list.append(SpawnListEntry(task_descriptor, spawned_continuation))

        # Return local reference to the interpreter.
        return ret
        
    def spawn_list_func(self, spawn_expr, args, num_outputs):
        # Create new continuation for the spawned function.
        spawned_continuation = self.build_spawn_continuation(spawn_expr, args)

        # Create swfs URI for continuation object.
        spawn_list_index = len(self.spawn_list)
        
        ret = []
        for i in range(0, num_outputs):
            ret.append(self.continuation.create_tasklocal_reference(SWLocalFutureReference(spawn_list_index, i)))

        # Append the new task definition to the spawn list.
        task_descriptor = {'handler': 'swi',
                           'inputs': {}, # _cont will be added later
                           'num_outputs': num_outputs
                          }
        
        # TODO: we could visit the spawn expression and try to guess what requirements
        #       and executors we need in here. 
        # TODO: should probably look at dereference wrapper objects in the spawn context
        #       and ship them as inputs.
        
        self.spawn_list.append(SpawnListEntry(task_descriptor, spawned_continuation))

        # Return local reference to the interpreter.
        return ret
    
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
        return self.continuation.create_tasklocal_reference(SWURLReference(urls))

    def lazy_dereference(self, ref):
        self.continuation_will_require.add(ref.id)
        self.continuation.mark_as_dereferenced(ref)
        return SWDereferenceWrapper(ref)
        
    def eager_dereference(self, ref):
        real_ref = self.continuation.resolve_tasklocal_ref(ref)
        if isinstance(real_ref, SWDataValue):
            return real_ref.value
        else:
            # TODO: consider loading this immediately if it is local.
            self.continuation_will_require.add(ref.id)
            self.continuation.mark_as_dereferenced(ref)
            raise ExecutionInterruption()
