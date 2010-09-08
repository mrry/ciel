# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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

'''
Created on 23 Feb 2010

@author: dgm36
'''
from skywriting.lang import ast
from skywriting.lang.visitors import ExpressionEvaluatorVisitor,\
    SWDynamicScopeWrapper
from skywriting.lang.resume import ContextAssignRR,\
    IndexedLValueRR
from skywriting.lang.datatypes import all_leaf_values

class LambdaFunction:
    
    def __init__(self, function):
        self.function = function
        self.captured_bindings = {}
        
    def call(self, args_list, stack, stack_base, context):
        return self.function(args_list)

class GlobalScope:
    
    def __init__(self):
        self.bindings = {}

    def has_binding_for(self, base_identifier):
        return base_identifier in self.bindings.keys()
        
    def value_of(self, base_identifier):
        return self.bindings[base_identifier]
    
GLOBAL_SCOPE = GlobalScope()

class SimpleContext:

    def __init__(self):
        self.contexts = []
        self.context_base = 0
        self.binding_bases = []
        self.enter_context()
    
    def abort(self):
        # TODO: Make this cleaner :-).
        self.contexts = None
        self.binding_bases = None
    
    def __repr__(self):
        return repr(self.contexts)
   
    def restart(self):
        self.context_base = 1
        self.binding_bases[self.context_base-1] = 1
    
    def bind_identifier(self, identifier, value):
        self.contexts[self.context_base-1][self.binding_bases[self.context_base-1]-1][identifier] = value
    
    def is_dynamic(self, identifier):
        return False
    
    def values(self):
        """
        Returns a list of all primitive values reachable in the current context.
        """
        for context in self.contexts:
            for binding_group in context:
                for value in binding_group:
                    for leaf in all_leaf_values(value):
                        yield leaf
    
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
                    resume_record.base_lvalue.extend([None for _ in range(0, index + 1 - list_length)])
                    
                resume_record.base_lvalue[index] = rvalue
                
                stack.pop()
                
            except:
                raise
            
        elif isinstance(lvalue, ast.FieldLValue):
            base_lvalue = GetBaseLValueBindingVisitor(self).visit(lvalue.base_lvalue, stack, stack_base + 1)
            base_lvalue[lvalue.field_name] = rvalue
        elif isinstance(lvalue, ast.Identifier):
            # XXX: do this to enable binding the name of a recursive function in its own context.
            self.bind_identifier(lvalue.identifier, rvalue)
        else:
            assert False
                
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
        
    def __repr__(self):
        return "TaskContext(local=..., wrapping=%s)" % (repr(self.wrapped_context))
        
    def abort(self):
        self.wrapped_context.abort()
        self.wrapped_context = None
   
    def restart(self):
        return self.wrapped_context.restart()
        
    def bind_identifier(self, identifier, value):
        return self.wrapped_context.bind_identifier(identifier, value)
    
    def is_dynamic(self, identifier):
        return identifier in self.tasklocal_bindings.keys() or self.wrapped_context.is_dynamic(identifier)
    
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
            if base_identifier in self.tasklocal_bindings.keys():
                return SWDynamicScopeWrapper(base_identifier)
            else:
                raise
        except:
            print "Error context[%d][%d]:" % (self.wrapped_context.context_base - 1, self.wrapped_context.binding_bases[self.wrapped_context.context_base-1] - 1), self.wrapped_context.contexts
            raise

    def value_of_dynamic_scope(self, base_identifier):
        return self.tasklocal_bindings[base_identifier]

    def remove_binding(self, base_identifier):
        return self.wrapped_context.remove_binding(base_identifier)

   
class GetBaseLValueBindingVisitor:
    
    def __init__(self, context):
        self.context = context
        
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node, stack, stack_base)
        
    def visit_IdentifierLValue(self, node, stack, stack_base):
        try:
            return self.context.contexts[self.context.context_base-1][self.context.binding_bases[self.context.context_base-1]-1][node.identifier]
        except KeyError:
            print self.context.contexts
            raise
    
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
