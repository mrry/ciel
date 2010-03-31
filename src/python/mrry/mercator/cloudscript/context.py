'''
Created on 23 Feb 2010

@author: dgm36
'''
from mrry.mercator.cloudscript import ast
from mrry.mercator.cloudscript.parser import CloudScriptParser
from mrry.mercator.cloudscript.visitors import StatementExecutorVisitor, ExpressionEvaluatorVisitor
from mrry.mercator.cloudscript.interpreter.resume import ContextAssignRR,\
    IndexedLValueRR


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

class SimpleContext(Context):

    def __init__(self):
        self.contexts = []
        self.context_base = 0
        self.binding_bases = []
        self.enter_context()
    
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
        # FIXME: may need to re-enter scopes.
        self.contexts[self.context_base-1].append({})
        self.binding_bases[self.context_base-1] += 1
    
    def exit_scope(self):
        self.contexts[self.context_base-1].pop()
        self.binding_bases[self.context_base-1] -= 1
        
    def enter_context(self, initial_bindings={}):
        if self.context_base == len(self.contexts):
            # FIXME: need to walk resumption....
            base_scope = {}
            base_scope["len"] = LambdaFunction(lambda x: len(x[0]))
            base_scope["range"] = LambdaFunction(lambda x: range(x[0], x[1]))
            
            for name, value in initial_bindings.items():
                base_scope[name] = value
            
            context = [base_scope]
            self.contexts.append(context)
            self.binding_bases.append(1)
        
        self.context_base += 1

    
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
    script = csp.parse(open('testscript.sw').read())
    
    print script
    
    
    import datetime

    
    for i in range(0, 1000):
        start = datetime.datetime.now()
        ctxt = SimpleContext()
        StatementExecutorVisitor(ctxt).visit(script, [], 0)
        end = datetime.datetime.now()
        print end - start
    
    
    print ctxt.contexts
    