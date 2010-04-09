from Queue import Queue
from threading import Lock, Condition, Thread
from mrry.mercator.cloudscript.visitors import ExpressionEvaluatorVisitor,\
    ExecutionInterruption, StatementExecutorVisitor, SWDereferenceWrapper,\
    SWFutureReference
from mrry.mercator.cloudscript.context import SimpleContext, LambdaFunction,\
    TaskContext
from mrry.mercator.cloudscript.parser import CloudScriptParser
from mrry.mercator.cloudscript import ast
import traceback
import sys

class SWThreadTerminator:
    pass
SW_THREAD_TERMINATOR = SWThreadTerminator()

class SWInterpreter:
    
    def __init__(self):
        # Process command-line options.
        self.script_filename = sys.argv[1]
            
    def start(self, num_threads=4):
        # Parse provided script file.
        script_ast = CloudScriptParser().parse(open(self.script_filename).read())
        
        # Initialise scheduler.
        self.scheduler = SWScheduler(num_threads)
        
        threads = []
        
        # Initialise task queue and worker thread pool.
        for i in range(0, num_threads):
            t = Thread(target=self.task_interpreter_main, args=())
            t.start()
            threads.append(t)
        
        # Spawn initial entry-point task.
        initial_task = SWInterpreterTask(self.scheduler, script_ast, is_root=True)
        self.scheduler.add_task(initial_task)
        
        # Block until completion.
        self.scheduler.join()
        for t in threads:
            t.join()
            
        print initial_task.result
    
    def task_interpreter_main(self):
        while True:
            
            if not self.scheduler.is_running:
#                print "No longer running :)"
                break
            
            task = self.scheduler.run_queue.get()
            
            if task is SW_THREAD_TERMINATOR:
#                print "Got thread terminator... no longer running :)"
                break
            
            # Need to provide some context variables for, e.g., list of future references.
            try:
#                print "Interpreting a task!"
                task.interpret()
            except Exception as e:
                traceback.print_exc()

                self.scheduler.halt()
    
class SWReference:
    
    pass
    
class SWScheduler:
    
    def __init__(self, num_threads):
        # Task handling members.
        self.run_queue = Queue()
        self.num_threads = num_threads
        self.is_running = True
        
        # Blocked task handling.
        self.references_blocking_tasks = {}
        
        # Data handling members.
        self.future_reference_list = []   
        self._lock = Lock()
        self._term_condition = Condition()

    def add_task(self, task):
        self.run_queue.put(task)

    def block_on_references(self, task, ref_ids):
        block_count = 0
        with self._lock:
            for ref_id in ref_ids:
                if self.future_reference_list[ref_id] is None:
                    try:
                        blocked_set = self.references_blocking_tasks[ref_id]
                    except KeyError:
                        blocked_set = set()
                        self.references_blocking_tasks[ref_id] = blocked_set                    
                    
                    blocked_set.add(task)
                    block_count += 1
        
        # All blocking references have been fulfilled in the race between faulting and blocking.
        if block_count == 0:
            self.run_queue.put(task)
            
    def allocate_future_reference(self):
        with self._lock:
            ret = len(self.future_reference_list)
            self.future_reference_list.append(None)
        return ret

    def resolve_future_reference(self, ref_id, real_reference):
        with self._lock:
            self.future_reference_list[ref_id] = real_reference
            
            # Now schedule any tasks that were blocked on this reference.
            try:
                for task in self.references_blocking_tasks[ref_id]:
                    task.reference_resolved(ref_id)
                    if task.runnable():
                        self.run_queue.put(task)
            except KeyError:
                pass

    def try_dereference(self, ref_id):
        with self._lock:
            if self.future_reference_list[ref_id] is not None:
                return self.future_reference_list[ref_id]
        return None

    def halt(self):
        with self._lock:
            self.is_running = False
            for i in range(0, self.num_threads):
                self.run_queue.put(SW_THREAD_TERMINATOR)
        
        with self._term_condition:
            self._term_condition.notifyAll()

    def join(self):
        with self._term_condition:
            while self.is_running:
                self._term_condition.wait()

    def spawn_func(self, callable, args):
        print "Spawning a task!!!"
        ref_id = self.allocate_future_reference()
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(callable, args))
        spawned_task = SWInterpreterTask(self, spawned_task_stmt, result_ref_id=ref_id)
        self.add_task(spawned_task)
        return SWFutureReference(ref_id)

    def spawn_list_func(self, callable, args, n):
        ref_id_list = [SWFutureReference(self.scheduler.allocate_future_reference()) for _ in range(0, n)]
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(callable, args))
        spawned_task = SWInterpreterTask(self.scheduler, spawned_task_stmt, result_ref_id_list=ref_id_list)
        self.scheduler.add_task(spawned_task)
        return ref_id_list

class SWInterpreterTask:
    
    def __init__(self, scheduler, task_expr, is_root=False, result_ref_id=None, result_ref_id_list=None):
        self.blocked_on = set()
        self.context = None
        self.stack = []
        
        self.scheduler = scheduler
        self.task_expr = task_expr
        
        self.is_root = is_root
        self.result_ref_id = None
        self.result_ref_id_list = None

        if result_ref_id is not None:
            self.result_ref_id = result_ref_id
        elif result_ref_id_list is not None:
            self.result_ref_id_list = result_ref_id_list
        if (not self.is_root) and result_ref_id is None and result_ref_id_list is None:
            raise 

    def interpret(self):
        if self.context is None:
            self.context = SimpleContext()
            
        task_context = TaskContext(self.context, self)
        # TODO: investigate when we might need to change the scheduler.
        task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.scheduler.spawn_func(x[0], x[1])))
        task_context.bind_tasklocal_identifier("spawn_list", LambdaFunction(lambda x: self.scheduler.spawn_list_func(x[0], x[1], x[2])))
        task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
    
        visitor = StatementExecutorVisitor(task_context)
        
        try:
            self.result = visitor.visit(self.task_expr, self.stack, 0)
            self.propagate_result(self.result)
            if self.is_root:
                self.scheduler.halt()
        except ExecutionInterruption as exi:
            print "Blocking on", self.blocked_on
            self.scheduler.block_on_references(self, self.blocked_on)
            
    def propagate_result(self, result):
        if self.result_ref_id is not None:
            # FIXME: may need to wrap this in a reference object.
            self.scheduler.resolve_future_reference(self.result_ref_id, result)
        elif self.result_ref_id_list is not None:
            if type(result) is list and len(result) == len(self.result_ref_id_list):
                for i in len(result):
                    # FIXME: or may need to wrap this in a dereference object! Probably this one.
                    self.scheduler.resolve_future_reference(self.result_ref_id_list[i], result[i])
            else:
                # Mismatch in the result type with what was expected.
                raise
            
    def lazy_dereference(self, ref):
        # TODO: consider whether this should be different from blocked_on.
        if ref.is_future:
            value = self.scheduler.try_dereference(ref.id)
            if value is not None:
                return value
            self.blocked_on.add(ref.id)
        else:
            # TODO: handle loading files, etc.
            pass

        return SWDereferenceWrapper(ref)

    def eager_dereference(self, ref):
        if ref.is_future:
            value = self.scheduler.try_dereference(ref.id)
            if value is not None:
                return value
            else:
                self.blocked_on.add(ref.id)
                raise ExecutionInterruption()
        else:
            # TODO: handle loading files, etc.
            pass
    def reference_resolved(self, ref_id):
        self.blocked_on.remove(ref_id)
        
    def runnable(self):
        return len(self.blocked_on) == 0
    
if __name__ == '__main__':
    SWInterpreter().start()