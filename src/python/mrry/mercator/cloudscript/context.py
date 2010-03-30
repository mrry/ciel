'''
Created on 23 Feb 2010

@author: dgm36
'''
from mrry.mercator.cloudscript import ast
from mrry.mercator.cloudscript.parser import CloudScriptParser
from mrry.mercator.cloudscript.visitors import StatementExecutorVisitor, Visitor,\
    ExpressionEvaluatorVisitor

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
        self.bindings = [{}]
        self.bindings[0]["len"] = LambdaFunction(lambda x: len(x[0]))
        self.bindings[0]["range"] = LambdaFunction(lambda x: range(x[0], x[1]))
    
    def bind_identifier(self, identifier, value):
        self.bindings[-1][identifier] = value
    
    def update_value(self, lvalue, rvalue):

        if isinstance(lvalue, ast.IdentifierLValue):
            self.bind_identifier(lvalue.base_identifier(), rvalue)
        elif isinstance(lvalue, ast.IndexedLValue):
            base_lvalue = GetBaseLValueBindingVisitor(self).visit(lvalue.base_lvalue)
            index = ExpressionEvaluatorVisitor(self).visit(lvalue.index)
            
            list_length = len(base_lvalue)
            if len(base_lvalue) < index + 1:
                base_lvalue.extend([None for i in range(0, index + 1 - list_length)])
                
            base_lvalue[index] = rvalue
        elif isinstance(lvalue, ast.FieldLValue):
            base_lvalue = GetBaseLValueBindingVisitor(self).visit(lvalue.base_lvalue)
            base_lvalue[lvalue.field_name] = rvalue
        else:
            raise
        
    def enter_subcontext(self):
        self.bindings.append({})
    
    def exit_subcontext(self):
        self.bindings.pop()
        
    def fresh_context(self):
        return SimpleContext()
        
    def has_binding_for(self, base_identifier):
        for binding in reversed(self.bindings):
            if base_identifier in binding.keys():
                return True
        return False
        
    def value_of(self, base_identifier):
        for binding in reversed(self.bindings):
            try:
                return binding[base_identifier]
            except KeyError:
                pass
    
    def remove_binding(self, base_identifier):
        
        print "Removing binding for identifier: %s" % base_identifier
        
        del self.bindings[-1][base_identifier]
    
class GetBaseLValueBindingVisitor(Visitor):
    
    def __init__(self, context):
        self.context = context
        
    def visit_IdentifierLValue(self, node):
        return self.context.bindings[-1][node.identifier]
    
    def visit_FieldLValue(self, node):
        return self.visit(node.base_lvalue)[node.field_name]
    
    def visit_IndexedLValue(self, node):
        index = ExpressionEvaluatorVisitor(self.context).visit(node.index)
        return self.visit(node.base_lvalue)[index]
    
if __name__ == '__main__':
    
    csp = CloudScriptParser()
    script = csp.parse(open('testscript.sw').read())
    
    print script
    
    
    import datetime
    start = datetime.datetime.now()
    
    ctxt = SimpleContext()
    StatementExecutorVisitor(ctxt).visit(script, [], 0)
    
    end = datetime.datetime.now()
    
    print end - start
    
    print ctxt.bindings