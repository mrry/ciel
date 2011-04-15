# Copyright (c) 2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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

from skywriting.runtime.exceptions import ReferenceUnavailableException,\
    ExecutionInterruption
from skywriting.lang.context import SimpleContext, TaskContext,\
    LambdaFunction
from skywriting.lang.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from skywriting.lang import ast
from skywriting.lang.parser import \
    SWScriptParser

class SWContinuation:
    
    def __init__(self, task_stmt, context):
        self.task_stmt = task_stmt
        self.current_local_id_index = 0
        self.stack = []
        self.context = context
      
    def __repr__(self):
        return "SWContinuation(task_stmt=%s, current_local_id_index=%s, stack=%s, context=%s)" % (repr(self.task_stmt), repr(self.current_local_id_index), repr(self.stack), repr(self.context))
    
class SafeLambdaFunction(LambdaFunction):
    
    def __init__(self, function, interpreter):
        LambdaFunction.__init__(self, function)
        self.interpreter = interpreter

    def call(self, args_list, stack, stack_base, context):
        safe_args = self.interpreter.do_eager_thunks(args_list)
        return LambdaFunction.call(self, safe_args, stack, stack_base, context)
    
def start_sw_script(swref, args, env):

    sw_file = retrieve_filename_for_ref(swref)
    parser = SWScriptParser()
    with open(sw_file, "r") as sw_fp:
        script = parser.parse(sw_fp.read())

    if script is None:
        raise Exception("Couldn't parse %s" % swref)

    cont = SWContinuation(script, SimpleContext())
    if env is not None:
        cont.context.bind_identifier('env', env)
    if args is not None:
        cont.context.bind_identifier('argv', args)
    return cont

def _run(self, task_private, task_descriptor, task_record):

    sw_private = task_private
    self.task_id = task_descriptor["task_id"]
    self.task_record = task_record

    try:
        save_continuation = task_descriptor["save_continuation"]
    except KeyError:
        save_continuation = False

    self.lazy_derefs = set()
    self.continuation = None
    self.result = None

    if "cont" in sw_private:
        self.continuation = retrieve_object_for_ref(sw_private["cont"], 'pickle')
    else:
        self.continuation = self.start_sw_script(sw_private["swfile_ref"], sw_private["start_args"], sw_private["start_env"])

    self.continuation.context.restart()
    task_context = TaskContext(self.continuation.context, self)
    
    task_context.bind_tasklocal_identifier("spawn", LambdaFunction(lambda x: self.spawn_func(x[0], x[1])))
    task_context.bind_tasklocal_identifier("spawn_exec", LambdaFunction(lambda x: self.spawn_exec_func(x[0], x[1], x[2])))
    task_context.bind_tasklocal_identifier("spawn_other", LambdaFunction(lambda x: self.spawn_other(x[0], x[1])))
    task_context.bind_tasklocal_identifier("__star__", LambdaFunction(lambda x: self.lazy_dereference(x[0])))
    task_context.bind_tasklocal_identifier("int", SafeLambdaFunction(lambda x: int(x[0]), self))
    task_context.bind_tasklocal_identifier("range", SafeLambdaFunction(lambda x: range(*x), self))
    task_context.bind_tasklocal_identifier("len", SafeLambdaFunction(lambda x: len(x[0]), self))
    task_context.bind_tasklocal_identifier("has_key", SafeLambdaFunction(lambda x: x[1] in x[0], self))
    task_context.bind_tasklocal_identifier("get_key", SafeLambdaFunction(lambda x: x[0][x[1]] if x[1] in x[0] else x[2], self))
    task_context.bind_tasklocal_identifier("exec", LambdaFunction(lambda x: self.exec_func(x[0], x[1], x[2])))
    task_context.bind_tasklocal_identifier("package", LambdaFunction(lambda x: package_lookup(self.task_record, self.block_store, x[0])))

    visitor = StatementExecutorVisitor(task_context)
    
    try:
        result = visitor.visit(self.continuation.task_stmt, self.continuation.stack, 0)

        # The script finished successfully

        # XXX: This is for the unusual case that we have a task fragment that runs 
        # to completion without returning anything.
        # Could maybe use an ErrorRef here, but this might not be erroneous if, 
        # e.g. the interactive shell is used.
        if result is None:
            result = SWErrorReference('NO_RETURN_VALUE', 'null')

        result_ref = ref_from_object(result, "json", task_descriptor["expected_outputs"][0])
        self.task_record.publish_ref(result_ref)
        
    except ExecutionInterruption, ei:
       
        spawn_task_helper(self.task_record, 
                          "swi", 
                          small_task=True, 
                          cont_delegated_output=task_descriptor["expected_outputs"][0], 
                          extra_dependencies=list(self.lazy_derefs), 
                          cont=self.continuation)

# TODO: Fix this?
#        if "save_continuation" in task_descriptor and task_descriptor["save_continuation"]:
#            self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 
#                                                                  'pickle', 
#                                                                  "%s:saved_cont" % task_descriptor["task_id"])
            
def spawn_func(self, spawn_expr, args):

    args = self.do_eager_thunks(args)
    spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
    cont = SWContinuation(spawned_task_stmt, SimpleContext())
    return spawn_task_helper(self.task_record, "swi", True, cont=cont)

def do_eager_thunks(self, args):

    def resolve_thunks_mapper(leaf):
        if isinstance(leaf, SWDereferenceWrapper):
            return self.eager_dereference(leaf.ref)
        else:
            return leaf

    return map_leaf_values(resolve_thunks_mapper, args)

def spawn_other(self, executor_name, executor_args_dict):
    # Args dict arrives from sw with unicode keys :(
    str_args = dict([(str(k), v) for (k, v) in executor_args_dict.items()])
    try:
        small_task = str_args.pop("small_task")
    except:
        small_task = False
    ret = spawn_task_helper(self.task_record, executor_name, small_task, **str_args)
    if isinstance(ret, SWRealReference):
        return ret
    else:
        return list(ret)

def spawn_exec_func(self, executor_name, args, num_outputs):
    return self.spawn_other(executor_name, {"args": args, "n_outputs": num_outputs})

def exec_func(self, executor_name, args, num_outputs):
    return self.spawn_other(executor_name, {"args": args, "n_outputs": num_outputs, "small_task": True})

def lazy_dereference(self, ref):
    self.lazy_derefs.add(ref)
    return SWDereferenceWrapper(ref)

def eager_dereference(self, ref):
    # For SWI, all decodes are JSON
    real_ref = self.task_record.retrieve_ref(ref)
    ret = retrieve_object_for_ref(real_ref, "json")
    self.lazy_derefs.discard(ref)
    return ret

def include_script(self, target_expr):
    if isinstance(target_expr, basestring):
        # Name may be relative to the local stdlib.
        if not target_expr.startswith('http'):
            target_url = 'file://%s' % os.path.join(self.stdlibbase, target_expr)
        else:
            target_url = target_expr
        target_ref = get_ref_for_url(target_url, 0, self.task_id)
    elif isinstance(target_expr, SWRealReference):    
        target_ref = target_expr
    else:
        raise BlameUserException('Invalid object %s passed as the argument of include', 'INCLUDE', logging.ERROR)

    try:
        script = retrieve_object_for_ref(target_ref, 'script')
    except:
        ciel.log.error('Error parsing included script', 'INCLUDE', logging.ERROR, True)
        raise BlameUserException('The included script did not parse successfully')
    return script

    
parser = optparse.OptionParser()
parser.add_option("-v", "--version", action="store_true", dest="version", default=False, help="Display version info")
parser.add_option("-w", "--write-fifo", action="store", dest="write_fifo", default=None, help="FIFO for communication towards Ciel")
parser.add_option("-r", "--read-fifo", action="store", dest="read_fifo", default=None, help="FIFO for communication from Ciel")
