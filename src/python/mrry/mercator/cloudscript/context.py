'''
Created on 23 Feb 2010

@author: dgm36
'''
from mrry.mercator.cloudscript import ast
from mrry.mercator.cloudscript.parser import CloudScriptParser
from mrry.mercator.cloudscript.visitors import StatementExecutorVisitor, ExpressionEvaluatorVisitor,\
    ExecutionInterruption
from mrry.mercator.cloudscript.resume import ContextAssignRR,\
    IndexedLValueRR
import cPickle
import os
import sys

class Context:
    
    def update_value(self, lvalue, rvalue):
        pass
    
    def value_of(self, name):
        pass
    
    def remove_binding(self, name):
        pass

class LambdaFunction:
    
    def __init__(self, function):
        self.function = function
        
    def call(self, args_list, stack, stack_base):
        return self.function(args_list)

class GlobalScope:
    
    def __init__(self):
        self.bindings = {}
        self.bindings["len"] = LambdaFunction(lambda x: len(x[0]))
        self.bindings["range"] = LambdaFunction(lambda x: range(x[0], x[1]))
            

    def has_binding_for(self, base_identifier):
        return base_identifier in self.bindings.keys()
        
    def value_of(self, base_identifier):
        return self.bindings[base_identifier]
    
GLOBAL_SCOPE = GlobalScope()

class SimpleContext(Context):

    def __init__(self):
        self.contexts = []
        self.context_base = 0
        self.binding_bases = []
        self.enter_context()
    
    def restart(self):
        self.context_base = 1
        self.binding_bases[self.context_base-1] = 1
    
    def bind_identifier(self, identifier, value):
        self.contexts[self.context_base-1][self.binding_bases[self.context_base-1]-1][identifier] = value
    
    def update_value(self, lvalue, rvalue, stack, stack_base):
        if isinstance(lvalue, ast.IdentifierLValue):
            self.bind_identifier(lvalue.base_identifier(), rvalue)
        elif isinstance(lvalue, ast.IndexedLValue):
            
            if stack_base == len(stack):
                resume_record = ContextAssignRR()
                stack.append(resume_record)
            else:
                resume_record = stack[stack_base]
                
            try:
            
                if resume_record.base_lvalue is None:
                    resume_record.base_lvalue = GetBaseLValueBindingVisitor(self).visit(lvalue.base_lvalue, stack, stack_base + 1)
                    
                index = ExpressionEvaluatorVisitor(self).visit(lvalue.index, stack, stack_base + 1)
              
                list_length = len(resume_record.base_lvalue)
                if len(resume_record.base_lvalue) < index + 1:
                    resume_record.base_lvalue.extend([None for i in range(0, index + 1 - list_length)])
                    
                resume_record.base_lvalue[index] = rvalue
                
                stack.pop()
                
            except:
                raise
            
        elif isinstance(lvalue, ast.FieldLValue):
            base_lvalue = GetBaseLValueBindingVisitor(self).visit(lvalue.base_lvalue, stack, stack_base + 1)
            base_lvalue[lvalue.field_name] = rvalue
        else:
            raise
                
    def enter_scope(self):
        if self.binding_bases[self.context_base-1] == len(self.contexts[self.context_base-1]):
            self.contexts[self.context_base-1].append({})
        
        self.binding_bases[self.context_base-1] += 1
    
    def exit_scope(self):
        self.contexts[self.context_base-1].pop()
        self.binding_bases[self.context_base-1] -= 1
        
    def enter_context(self, initial_bindings={}):
        if self.context_base == len(self.contexts):

            base_scope = {}
            
            for name, value in initial_bindings.items():
                base_scope[name] = value
            
            context = [base_scope]
            self.contexts.append(context)
            self.binding_bases.append(1)
        
        self.context_base += 1
        self.binding_bases[self.context_base-1] = 1
    
    def exit_context(self):
        self.contexts.pop()
        self.context_base -= 1
        self.binding_bases.pop()
                
    def has_binding_for(self, base_identifier):
        for binding in reversed(self.contexts[self.context_base-1][0:self.binding_bases[self.context_base-1]]):
            if base_identifier in binding.keys():
                return True
        return False
                
    def value_of(self, base_identifier):
        for binding in reversed(self.contexts[self.context_base-1][0:self.binding_bases[self.context_base-1]]):
            try:
                return binding[base_identifier]
            except KeyError:
                pass
        raise KeyError(base_identifier)
    
    def remove_binding(self, base_identifier):
        del self.contexts[self.context_base-1][self.binding_bases[self.context_base-1]-1][base_identifier]
    
class TaskContext:
    
    def __init__(self, wrapped_context, task):
        self.wrapped_context = wrapped_context
        self.task = task
        self.tasklocal_bindings = {}
   
    def restart(self):
        return self.wrapped_context.restart()
        
    def bind_identifier(self, identifier, value):
        return self.wrapped_context.bind_identifier(identifier, value)
    
    def bind_tasklocal_identifier(self, identifier, value):
        self.tasklocal_bindings[identifier] = value
    
    def eager_dereference(self, reference):
        return self.task.eager_dereference(reference)
    
    def update_value(self, lvalue, rvalue, stack, stack_base):
        return self.wrapped_context.update_value(lvalue, rvalue, stack, stack_base)
                
    def enter_scope(self):
        return self.wrapped_context.enter_scope()
    
    def exit_scope(self):
        return self.wrapped_context.exit_scope()
    
    def enter_context(self, initial_bindings={}):
        return self.wrapped_context.enter_context(initial_bindings)
            
    def exit_context(self):
        return self.wrapped_context.exit_context()
                
    def has_binding_for(self, base_identifier):
        ret = self.wrapped_context.has_binding_for(base_identifier)
        if ret:
            return ret
        else:
            return GLOBAL_SCOPE.has_binding_for(base_identifier) or base_identifier in self.tasklocal_bindings.keys()
        
    def value_of(self, base_identifier):
        try:
            return self.wrapped_context.value_of(base_identifier)
        except KeyError:
            pass
        
        try:
            return GLOBAL_SCOPE.value_of(base_identifier)
        except:
            pass
        
        try:
            return self.tasklocal_bindings[base_identifier]
        except:
            print "Error context[%d][%d]:" % (self.wrapped_context.context_base - 1, self.wrapped_context.binding_bases[self.wrapped_context.context_base-1] - 1), self.wrapped_context.contexts
            raise

    def remove_binding(self, base_identifier):
        return self.wrapped_context.remove_binding(base_identifier)
   
class GetBaseLValueBindingVisitor:
    
    def __init__(self, context):
        self.context = context
        
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node, stack, stack_base)
        
    def visit_IdentifierLValue(self, node, stack, stack_base):
        return self.context.contexts[self.context.context_base-1][self.context.binding_bases[self.context.context_base-1]-1][node.identifier]
    
    def visit_FieldLValue(self, node, stack, stack_base):
        return self.visit(node.base_lvalue, stack, stack_base)[node.field_name]
    
    def visit_IndexedLValue(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = IndexedLValueRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            if resume_record.index is None:
                resume_record.index = ExpressionEvaluatorVisitor(self.context).visit(node.index, stack, stack_base + 1)
            lvalue = self.visit(node.base_lvalue, stack, stack_base + 1)
            stack.pop()
            return lvalue[resume_record.index]
            
        except:
            raise
        
if __name__ == '__main__':
    
    csp = CloudScriptParser()
    
    try:
        filename = sys.argv[1]
    except:
        filename = "testscript2.sw"
    
    script = csp.parse(open(filename).read())
        
    import datetime

    
    for i in range(0, 1):
        start = datetime.datetime.now()
        ctxt = SimpleContext()
        
        stack = []
        while True:
            try:
                StatementExecutorVisitor(ctxt).visit(script, stack, 0)
                break
            except ExecutionInterruption:
                stack = cPickle.loads((cPickle.dumps(stack)))
                ctxt = cPickle.loads((cPickle.dumps(ctxt)))
                ctxt.restart()
            
        end = datetime.datetime.now()
        print end - start
    
    
    print ctxt.contexts
    
