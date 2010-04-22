'''
Created on 13 Apr 2010

@author: dgm36
'''
from mrry.mercator.runtime.plugins import AsynchronousExecutePlugin
from mrry.mercator.cloudscript.context import SimpleContext, TaskContext,\
    LambdaFunction
from mrry.mercator.cloudscript.datatypes import all_leaf_values, map_leaf_values
from mrry.mercator.cloudscript.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from mrry.mercator.cloudscript import ast
from mrry.mercator.runtime.executors import JavaExecutor
from mrry.mercator.runtime.exceptions import ReferenceUnavailableException,\
    FeatureUnavailableException, ExecutionInterruption
import cherrypy
import logging
from mrry.mercator.runtime.references import SWDataValue, SWURLReference,\
    SWLocalDataFile, build_reference_from_tuple, SWRealReference,\
    SWLocalFutureReference, SWGlobalFutureReference

class TaskExecutorPlugin(AsynchronousExecutePlugin):
    
    def __init__(self, bus, block_store, master_proxy, execution_features, num_threads=1):
        AsynchronousExecutePlugin.__init__(self, bus, num_threads, "execute_task")
        self.block_store = block_store
        self.master_proxy = master_proxy
        self.execution_features = execution_features
    
    def handle_input(self, input):
        assert input['handler'] == 'swi'
        return self.handle_swi_task(input)
        
    def handle_swi_task(self, task_descriptor):
        print '!!! Starting SWI task', task_descriptor['task_id']
        try:     
            task_id = task_descriptor['task_id']
        except KeyError:
            return
                
        try:
            interpreter = SWRuntimeInterpreterTask(task_descriptor, self.block_store, self.execution_features)
            # TODO: remove redundant arguments.
            interpreter.fetch_inputs(self.block_store)
            interpreter.interpret()
            interpreter.spawn_all(self.block_store, self.master_proxy)
            interpreter.commit_result(self.block_store, self.master_proxy)
        except:
            cherrypy.log.error('Error during SWI task execution', 'SWI', logging.ERROR, True)
            self.master_proxy.failed_task(task_id)

class ReferenceTableEntry:
    
    def __init__(self, reference):
        self.reference = reference
        self.is_dereferenced = False
        self.is_execd = False
        self.is_returned = False
        
    def __repr__(self):
        return 'ReferenceTableEntry(%s, d=%s, e=%s, r=%s)' % (repr(self.reference), repr(self.is_dereferenced), repr(self.is_execd), repr(self.is_returned))
        
class SpawnListEntry:
    
    def __init__(self, task_descriptor, continuation):
        self.task_descriptor = task_descriptor
        self.continuation = continuation
    
class SWContinuation:
    
    def __init__(self, task_stmt, context=SimpleContext()):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = context
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
        self.reference_table[ref.index].is_dereferenced = True
    def is_marked_as_dereferenced(self, id):
        return self.reference_table[id].is_dereferenced
    def mark_as_execd(self, ref):
        self.reference_table[ref.index].is_execd = True
    def is_marked_as_execd(self, id):
        return self.reference_table[id].is_execd
    def mark_as_returned(self, ref):
        self.reference_table[ref.index].is_returned = True
    def is_marked_as_returned(self, id):
        return self.reference_table[id].is_returned
        
    def rewrite_reference(self, id, real_ref):
        self.reference_table[id].reference = real_ref
        
    def resolve_tasklocal_reference_with_index(self, index):
        return self.reference_table[index].reference
    def resolve_tasklocal_reference_with_ref(self, ref):
        return self.reference_table[ref.index].reference

class SWLocalReference:
    """
    A primitive object used in the interpreter, and returned from functions like
    ref() and spawn(). Contains an index into the continuation's reference table,
    which identifies the real reference object.
    """
    
    def __init__(self, index):
        self.index = index

    
class SWRuntimeInterpreterTask:
    
    def __init__(self, task_descriptor, block_store, execution_features): # scheduler, task_expr, is_root=False, result_ref_id=None, result_ref_id_list=None, context=None, condvar=None):
        self.task_id = task_descriptor['task_id']
        self.expected_outputs = task_descriptor['expected_outputs']
        self.inputs = task_descriptor['inputs']


        self.block_store = block_store
        self.execution_features = execution_features

        self.spawn_list = []
        
        self.continuation = None
        self.result = None
        self.spawn_task_result_global_ids = None
        
    def fetch_inputs(self, block_store):
        continuation_ref = None
        parsed_inputs = {}
        
        for local_id, ref_tuple in self.inputs.items():
            ref = build_reference_from_tuple(ref_tuple)
            if local_id == '_cont':
                continuation_ref = ref
            else:
                parsed_inputs[int(local_id)] = ref
        
        assert isinstance(continuation_ref, SWURLReference)
        self.continuation = block_store.retrieve_object_by_url(continuation_ref.urls[0], 'pickle')
        
        for local_id, ref in parsed_inputs.items():
            if self.continuation.is_marked_as_dereferenced(local_id):
                if isinstance(ref, SWDataValue):
                    self.continuation.rewrite_reference(local_id, ref)
                else:
                    assert isinstance(ref, SWURLReference)
                    value = block_store.retrieve_object_by_url(ref.urls[0], 'json')
                    self.continuation.rewrite_reference(local_id, SWDataValue(value))
            elif self.continuation.is_marked_as_execd(local_id):
                if isinstance(ref, SWDataValue):
                    url = block_store.store_object(ref.value, 'json')
                    filename = block_store.retrieve_filename_by_url(url)
                elif isinstance(ref, SWURLReference):
                    filename = block_store.retrieve_filename_by_url(ref.urls[0])
                elif isinstance(ref, SWLocalDataFile):
                    filename = ref.filename
                self.continuation.rewrite_reference(local_id, SWLocalDataFile(filename))
            else:
                assert False

    def convert_tasklocal_to_real_reference(self, value):
        if isinstance(value, SWLocalReference):
            return self.continuation.resolve_tasklocal_reference_with_ref(value)
        else:
            return value

    def convert_real_to_tasklocal_reference(self, value):
        if isinstance(value, SWRealReference):
            return self.continuation.create_tasklocal_reference(value)
        else:
            return value

    def interpret(self):
        self.continuation.context.restart()
        task_context = TaskContext(self.continuation.context, self)
        
        task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.spawn_func(x[0], x[1])))
        task_context.bind_tasklocal_identifier("spawn_list", LambdaFunction(lambda x: self.spawn_list_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
        task_context.bind_tasklocal_identifier("exec", LambdaFunction(lambda x: self.exec_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("ref", LambdaFunction(lambda x: self.make_reference(x)))
        visitor = StatementExecutorVisitor(task_context)
        
        try:
            self.result = visitor.visit(self.continuation.task_stmt, self.continuation.stack, 0)
            
        except ExecutionInterruption as ei:
            # Need to add a continuation task to the spawn list.
            cont_deps = {}
            for index in self.continuation.reference_table.keys():
                if (not isinstance(self.continuation.resolve_tasklocal_reference_with_index(index), SWDataValue)) and \
                   (self.continuation.is_marked_as_dereferenced(index) or self.continuation.is_marked_as_execd(index)):
                    cont_deps[index] = self.continuation.resolve_tasklocal_reference_with_index(index).as_tuple()
            cont_task_descriptor = {'handler': 'swi',
                                    'inputs': cont_deps, # _cont will be added at spawn time.
                                    'expected_outputs': self.expected_outputs}
            
            if isinstance(ei, FeatureUnavailableException):
                cont_task_descriptor['require_features'] = [ei.feature_name]
            
            self.spawn_list.append(SpawnListEntry(cont_task_descriptor, self.continuation))
            return
            
        except Exception:
            raise

    def spawn_all(self, block_store, master_proxy):
        current_batch = []
        
        self.spawn_task_result_global_ids = []
        
        current_index = 0
        while current_index < len(self.spawn_list):
            
            must_wait = False
            current_cont = self.spawn_list[current_index].continuation
            current_desc = self.spawn_list[current_index].task_descriptor
            
            for local_id, ref_table_entry in current_cont.reference_table.items():
                if isinstance(ref_table_entry.reference, SWLocalFutureReference):
                    # if unavailable in the local lookup table (from previous spawn batches), must wait;
                    # else rewrite the reference.
                    spawn_list_index = ref_table_entry.reference.spawn_list_index
                    if spawn_list_index >= len(self.spawn_task_result_global_ids):
                        must_wait = True
                        break
                    else:
                        current_cont.rewrite_reference(local_id, SWGlobalFutureReference(self.spawn_task_result_global_ids[spawn_list_index]))

            rewritten_inputs = {}
            for local_id, ref_tuple in current_desc['inputs'].items():
                if ref_tuple[0] == 'lfut':
                    if ref_tuple[1] >= len(self.spawn_task_result_global_ids):
                        must_wait = True
                        break
                    else:
                        rewritten_inputs[local_id] = SWGlobalFutureReference(self.spawn_task_result_global_ids[ref_tuple[1]]).as_tuple()
                else:
                    rewritten_inputs[local_id] = ref_tuple
            
            current_desc['inputs'] = rewritten_inputs
                
                
            if must_wait:
                
                # Fire off the current batch.
                batch_result_ids = master_proxy.spawn_tasks(self.task_id, current_batch)
                
                # Update a local structure containing all of the spawn/global ids so far.
                self.spawn_task_result_global_ids.extend(batch_result_ids)
                
                # Iterate again on the same index.
                current_batch = []
                continue
                
            else:
                
                # Store the continuation and add it to the task descriptor.
                cont_url = block_store.store_object(current_cont, 'pickle')
                self.spawn_list[current_index].task_descriptor['inputs']['_cont'] = SWURLReference([cont_url]).as_tuple()
            
                # Current task is now ready to be spawned.
                current_batch.append(self.spawn_list[current_index].task_descriptor)
                current_index += 1
            
        if len(current_batch) > 0:
            
            # Fire off the current batch.
            batch_result_ids = master_proxy.spawn_tasks(self.task_id, current_batch)
            
            self.spawn_task_result_global_ids.extend(batch_result_ids)

        print "Spawn lengths:", len(self.spawn_list), len(self.spawn_task_result_global_ids)
        
    def commit_result(self, block_store, master_proxy):
        if self.result is None:
            master_proxy.commit_task(self.task_id, {})
            print '### Successfully yielded in task', self.task_id, self.expected_outputs
            return
        
        
        for local_id, ref_table_entry in self.continuation.reference_table.items():
            if isinstance(ref_table_entry.reference, SWLocalFutureReference):
                print "About to rewrite!!!"
                
                # if unavailable in the local lookup table (from previous spawn batches), must wait;
                # else rewrite the reference.
                spawn_list_index = ref_table_entry.reference.spawn_list_index

                # All subtasks have been spawned and we're completed so this assertion must hold.
                print spawn_list_index, 
                assert spawn_list_index < len(self.spawn_task_result_global_ids)


                self.continuation.rewrite_reference(local_id, SWGlobalFutureReference(self.spawn_task_result_global_ids[spawn_list_index]))
        
        real_result = map_leaf_values(self.convert_tasklocal_to_real_reference, self.result)
        
        commit_bindings = {}
        

        if real_result is list and self.expected_outputs is list:
            assert len(real_result) >= len(self.expected_outputs)
            for i, output in enumerate(self.expected_outputs):
                # TODO: handle the case where we return a spawned reference (tail-recursion).
                #       ...could make this happen in visit_Return with an "is_returned" Ref Table Entry.
                # FIXME: we should never have a LocalReference after doing this.
                commit_bindings[output] = real_result[i].urls
            
        elif self.expected_outputs is not list:
            print "*!*!*!*", real_result
            result_url = block_store.store_object(real_result, 'json')
            commit_bindings[self.expected_outputs] = [result_url]
        
        else:
            assert False
        
        master_proxy.commit_task(self.task_id, commit_bindings)

        print '### Successfully completed task', self.task_id, self.expected_outputs

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
        executor = self.execution_features.get_executor(executor_name, args, self.continuation, num_outputs)
        executor.execute(self.block_store)
        return executor.output_refs

    def make_reference(self, urls):
        # TODO: should we add this to local_outputs?
        return self.continuation.create_tasklocal_reference(SWURLReference(urls))

    def lazy_dereference(self, ref):
        self.continuation.mark_as_dereferenced(ref)
        return SWDereferenceWrapper(ref)
        
    def eager_dereference(self, ref):
        real_ref = self.continuation.resolve_tasklocal_reference_with_ref(ref)
        if isinstance(real_ref, SWDataValue):
            return map_leaf_values(self.convert_real_to_tasklocal_reference, real_ref.value)
        elif isinstance(real_ref, SWURLReference):
            value = self.block_store.retrieve_object_by_url(real_ref.urls[0])
            dv_ref = SWDataValue(value)
            self.continuation.rewrite_reference(ref.id, dv_ref)
            return map_leaf_values(self.convert_real_to_tasklocal_reference, value)
        else:
            self.continuation.mark_as_dereferenced(ref)
            raise ReferenceUnavailableException(ref, self.continuation)
