
from skywriting.runtime.exceptions import ExecutionInterruption, ReferenceUnavailableException,\
    BlameUserException
from skywriting.lang.context import SimpleContext, TaskContext,\
    LambdaFunction
from skywriting.lang.visitors import \
    StatementExecutorVisitor, SWDereferenceWrapper
from skywriting.lang import ast
from skywriting.lang.parser import \
    SWScriptParser
from skywriting.lang.datatypes import map_leaf_values
from skywriting.lang.parser import CloudScriptParser
from shared.references import SWErrorReference, decode_datavalue_string,\
    SWReferenceJSONEncoder, json_decode_object_hook, SWRealReference,\
    encode_datavalue
from shared.io_helpers import MaybeFile

import simplejson
import pickle
import httplib2
import sys
import os

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
    
class SkywritingTask:
    
    def __init__(self, ciel_runtime, stdlib_base):
        self.ciel_runtime = ciel_runtime
        self.stdlib_base = stdlib_base
    
    ### Support functions (TODO: factor into generic Python bindings?)
    
    def get_ref(self, ref, string_decode, file_decode):
        runtime_response = self.ciel_runtime.synchronous_request("open_ref", {"ref": ref})
        if "error" in runtime_response:
            raise ReferenceUnavailableException(ref)
        elif "strdata" in runtime_response:
            return string_decode(decode_datavalue_string(runtime_response["strdata"]))
        elif "filename" in runtime_response:
            with open(runtime_response["filename"], "r") as fp:
                return file_decode(fp)
            
    def get_string_for_ref(self, ref):
        return self.get_ref(ref, lambda str: str, lambda fp: fp.read())
    
    def unpickle_ref(self, ref):
        return self.get_ref(ref, pickle.loads, pickle.load)

    def unjson_ref(self, ref):
        return self.get_ref(ref, lambda str: simplejson.loads(str, object_hook=json_decode_object_hook), lambda fp: simplejson.load(fp, object_hook=json_decode_object_hook))
    
    def open_output(self, index):
        runtime_response = self.ciel_runtime.synchronous_request("open_output", {"index": index})
        return open(runtime_response["filename"], "w")
    
    def write_output(self, index, write_callback):
        with MaybeFile(open_callback = lambda: self.open_output(index)) as fp:
            write_callback(fp)
        if fp.real_fp is not None:
            ret = self.ciel_runtime.synchronous_request("close_output", {"index": index, "size": fp.bytes_written})
        else:
            ret = self.ciel_runtime.synchronous_request("publish_string", {"index": index, "str": encode_datavalue(fp.str)})
        return ret["ref"]
            
    def get_fresh_output(self, prefix=""):
        return self.ciel_runtime.synchronous_request("allocate_output", {"prefix": prefix})["index"]
    
    ### End support functions
            
    def start_sw_script(self, swref, args, env):
    
        sw_str = self.get_string_for_ref(swref)
        parser = SWScriptParser()
        script = parser.parse(sw_str)
    
        if script is None:
            raise Exception("Couldn't parse %s" % swref)
    
        cont = SWContinuation(script, SimpleContext())
        if env is not None:
            cont.context.bind_identifier('env', env)
        if args is not None:
            cont.context.bind_identifier('argv', args)
        return cont
    
    def run(self):

        sw_private = self.ciel_runtime.await_message("start_task")
   
        self.lazy_derefs = set()
        self.continuation = None
        self.result = None
    
        if "cont" in sw_private:
            self.continuation = self.unpickle_ref(sw_private["cont"])
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
        task_context.bind_tasklocal_identifier("package", LambdaFunction(lambda x: self.package_lookup(x[0])))
    
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
    
            self.write_output(0, lambda fp: simplejson.dump(result, fp, cls=SWReferenceJSONEncoder))
            
        except ExecutionInterruption:

            cont_output = self.get_fresh_output("cont")
            cont_ref = self.write_output(cont_output, lambda fp: pickle.dump(self.continuation, fp))
            
            self.ciel_runtime.send_message("tail_spawn", {"executor_name": "swi",
                                                          "cont_ref": cont_ref,
                                                          "small_task": True,
                                                          "extra_dependencies": list(self.lazy_derefs)
                                                          }
                                           )
   
    # TODO: Fix this?
    #        if "save_continuation" in task_descriptor and task_descriptor["save_continuation"]:
    #            self.save_cont_uri, _ = self.block_store.store_object(self.continuation, 
    #                                                                  'pickle', 
    #                                                                  "%s:saved_cont" % task_descriptor["task_id"])
                
    def spawn_func(self, spawn_expr, args):
    
        args = self.do_eager_thunks(args)
        spawned_task_stmt = ast.Return(ast.SpawnedFunction(spawn_expr, args))
        cont = SWContinuation(spawned_task_stmt, SimpleContext())
        cont_output = self.get_fresh_output("cont")
        cont_ref = self.write_output(cont_output, lambda fp: pickle.dump(cont, fp))
        return self.ciel_runtime.synchronous_request("spawn", {"executor_name": "swi", "small_task": True, "cont_ref": cont_ref})
    
    def do_eager_thunks(self, args):
    
        def resolve_thunks_mapper(leaf):
            if isinstance(leaf, SWDereferenceWrapper):
                return self.eager_dereference(leaf.ref)
            else:
                return leaf
    
        return map_leaf_values(resolve_thunks_mapper, args)
    
    def spawn_other(self, executor_name, executor_args_dict):
        # Args dict arrives from sw with unicode keys :(
        executor_args_dict["executor_name"] = executor_name
        str_args = dict([(str(k), v) for (k, v) in executor_args_dict.items()])
        ret = self.ciel_runtime.synchronous_request("spawn", str_args)
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
        ret = self.unjson_ref(ref)
        self.lazy_derefs.discard(ref)
        return ret
    
    def package_lookup(self, key):
        return self.ciel_runtime.synchronous_request("package_lookup", {"key": key})["value"]
    
    def include_script(self, target_expr):
        if isinstance(target_expr, basestring):
            # Name may be relative to the local stdlib.
            if target_expr.startswith('http'):
                try:
                    _, script_str = httplib2.Http().request(target_expr)
                except Exception as e:
                    print >>sys.stderr, "Include HTTP failed:", repr(e)
                    raise
            else:
                try:
                    with open(os.path.join(self.stdlib_base, target_expr), "r") as fp:
                        script_str = fp.read()
                except Exception as e:
                    print >>sys.stderr, "Include file failed:", repr(e)
                    raise
        elif isinstance(target_expr, SWRealReference):    
            script_str = self.get_string_for_ref(target_expr)
        else:
            raise BlameUserException('Invalid object %s passed as the argument of include')

        try:
            return CloudScriptParser().parse(script_str)
        except Exception as e:
            print >>sys.stderr, 'Error parsing included script:', repr(e)
            raise BlameUserException('The included script did not parse successfully')
        