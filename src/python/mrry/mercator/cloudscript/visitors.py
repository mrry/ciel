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
from mrry.mercator.cloudscript.resume import BinaryExpressionRR,\
    FunctionCallRR, ListRR, DictRR, StatementListRR, DoRR, IfRR, WhileRR, ForRR,\
    ListIndexRR, AssignmentRR, ReturnRR, PlusRR, LessThanOrEqualRR, EqualRR,\
    StarRR, ForceEvalRR, PlusAssignmentRR
from mrry.mercator.cloudscript.datatypes import map_leaf_values

indent = 0

class Visitor:
    
    def visit(self, node):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node)

#class SWFutureReference:
#    
#    def __init__(self, ref_id):
#        self.is_future = True
#        self.id = ref_id

#class SWDataReference:
#    
#    def __init__(self, urls):
#        self.is_future = False
#        self.urls = urls

class SWDereferenceWrapper:
    
    def __init__(self, ref):
        self.ref = ref

class SWDynamicScopeWrapper:
    
    def __init__(self, identifier):
        self.identifier = identifier
        self.captured_bindings = {}

    def call(self, args_list, stack, stack_base, context):
        actual_function = context.value_of_dynamic_scope(self.identifier)
        return actual_function.call(args_list, stack, stack_base, context)

class StatementResult:
    pass
RESULT_BREAK = StatementResult()
RESULT_CONTINUE = StatementResult()

class StatementExecutorVisitor(Visitor):
    
    def __init__(self, context):
        self.context = context
        
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (node.__class__.__name__, ))(node, stack, stack_base)
        
    def visit_statement_list(self, statements, stack, stack_base):
        if stack_base == len(stack):
            resume_record = StatementListRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        ret = None
            
        try:

            for i in range(resume_record.current_statement_index, len(statements)):
                resume_record.current_statement_index = i
                ret = self.visit(statements[i], stack, stack_base + 1)
                if ret is not None:
                    break
                
        except:
            raise
            
        stack.pop()
        return ret
    
    def visit_Assignment(self, node, stack, stack_base):

        if stack_base == len(stack):
            resume_record = AssignmentRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            
            if resume_record.rvalue is None:
                resume_record.rvalue = ExpressionEvaluatorVisitor(self.context).visit(node.rvalue, stack, stack_base + 1)

            self.context.update_value(node.lvalue, resume_record.rvalue, stack, stack_base + 1)
        
        except:
            raise
        
        stack.pop()
        return None
    
    def visit_PlusAssignment(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = PlusAssignmentRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        try:
            if resume_record.rvalue is None:
                resume_record.rvalue = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.rvalue, stack, stack_base + 1)
            prev = self.context.value_of(node.lvalue)
            if isinstance(prev, list):
                prev.append(resume_record.rvalue)
            else:
                self.context.update_value(node.lvalue, prev + resume_record.rvalue, stack, stack_base + 1)
        except:
            raise
        stack.pop()
        return None

    def visit_Break(self, node, stack, stack_base):
        return RESULT_BREAK
    
    def visit_Continue(self, node, stack, stack_base):
        return RESULT_CONTINUE
    
    def visit_Do(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = DoRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        
        try:

            while True:
                # N.B. We may be resuming after having completed the body, but faulting on the condition test.
                if not resume_record.done_body:
                    ret = self.visit_statement_list(node.body, stack, stack_base + 1)
                    resume_record.done_body = True
                    if ret is not None:
                        if ret is RESULT_BREAK:
                            ret = None
                            break
                        elif ret is RESULT_CONTINUE:
                            continue
                        else:
                            break        

                condition = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.condition, stack, stack_base + 1)

                if not condition:
                    ret = None
                    break
                
                resume_record.done_body = False

        except:
            raise

        stack.pop()
        return ret
    
    def visit_If(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = IfRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            if resume_record.condition is None:
                resume_record.condition = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.condition, stack, stack_base + 1)
            
            if resume_record.condition:
                ret = self.visit_statement_list(node.true_body, stack, stack_base + 1)
            elif node.false_body is not None:
                ret = self.visit_statement_list(node.false_body, stack, stack_base + 1)
            else:
                ret = None
            
        except:
            raise
        
        stack.pop()
        return ret

    def visit_For(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = ForRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            
            if resume_record.iterator is None:
                resume_record.iterator = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.iterator, stack, stack_base + 1)

            indexer_lvalue = node.indexer       
            ret = None         
            for i in range(resume_record.i, len(resume_record.iterator)):
                resume_record.i = i
                self.context.update_value(indexer_lvalue, resume_record.iterator[i], stack, stack_base + 1)
                ret = self.visit_statement_list(node.body, stack, stack_base + 1)
                    
                if ret is not None:
                    if ret is RESULT_BREAK:
                        ret = None
                        break
                    elif ret is RESULT_CONTINUE:
                        continue
                    else:
                        break                
                
        except:
            raise
        
        stack.pop()
        return ret

    def convert_wrapper_to_eager_dereference(self, value):
        if isinstance(value, SWDereferenceWrapper):
            return self.context.eager_dereference(value.ref)
        else:
            return value

    def visit_Return(self, node, stack, stack_base):
        if node.expr is None:
            return None
        
        if stack_base == len(stack):
            resume_record = ReturnRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        
        try: 
            if resume_record.ret is None:
                resume_record.ret = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.expr, stack, stack_base + 1)
            
            # We must scan through the return value to see if it contains any dereferenced references, and if so, yield so these can be fetched.
            eager_derefd_val = map_leaf_values(self.convert_wrapper_to_eager_dereference, resume_record.ret)
            
            stack.pop()
            return eager_derefd_val
            
        except:
            raise

    def visit_While(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = WhileRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            
            while True:
                
                if not resume_record.done_condition:
                    condition = ExpressionEvaluatorVisitor(self.context).visit_and_force_eval(node.condition, stack, stack_base + 1)
                    if not condition:
                        ret = None
                        break
                    resume_record.done_condition = True
        
                ret = self.visit_statement_list(node.body, stack, stack_base + 1)
                if ret is not None:
                    if ret is RESULT_BREAK:
                        ret = None
                        break
                    elif ret is RESULT_CONTINUE:
                        continue
                    else:
                        break
                    
                resume_record.done_condition = False
        
        except:
            raise
        
        stack.pop()
        return ret

    def visit_Script(self, node, stack, stack_base):
        return self.visit_statement_list(node.body, stack, stack_base)

    def visit_NamedFunctionDeclaration(self, node, stack, stack_base):
        func = UserDefinedFunction(self.context, node)
        self.context.update_value(node.name, func, stack, stack_base)
        return None

class ExpressionEvaluatorVisitor:
    
    def __init__(self, context):
        self.context = context
    
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (node.__class__.__name__, ))(node, stack, stack_base)
    
    def visit_and_force_eval(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = ForceEvalRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.maybe_wrapped is None:
            resume_record.maybe_wrapped = self.visit(node, stack, stack_base + 1)

        try:
            if isinstance(resume_record.maybe_wrapped, SWDereferenceWrapper):
                ret = self.context.eager_dereference(resume_record.maybe_wrapped.ref)
            elif isinstance(resume_record.maybe_wrapped, SWDynamicScopeWrapper):
                ret = self.context.value_of_dynamic_scope(resume_record.maybe_wrapped.identifier)
            else:
                ret = resume_record.maybe_wrapped

            stack.pop()
            return ret

        except:
            raise
                
    def visit_And(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left and rexpr

        except:
            raise
    
    def visit_Constant(self, node, stack, stack_base):
        return node.value
    
    def visit_Dereference(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = StarRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.reference, stack, stack_base + 1)
            
            star_function = self.context.value_of('__star__')
            
            ret = star_function.call([resume_record.left], stack, stack_base + 1, self.context)
            
            stack.pop()
            return ret
        
        except:
            raise

    def visit_Dict(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = DictRR(len(node.items))
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        
        try:    
            ret = {}
            for i in range(len(node.items)):
                if resume_record.contents[i] is None:
                    resume_record.contents[i] = self.visit(node.items[i], stack, stack_base + 1)
                
                key, value = resume_record.contents[i]
                ret[key] = value
            stack.pop()
            
            return ret
        except:
            raise
    
    def visit_Equal(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = EqualRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left == rexpr

        except:
            raise
    
    def visit_SpawnedFunction(self, node, stack, stack_base):
        return node.function.call(node.args, stack, stack_base, self.context)
        
    def visit_FunctionCall(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = FunctionCallRR(len(node.args))
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            for i in range(len(node.args)):
                if resume_record.args[i] is None:
                    resume_record.args[i] = self.visit(node.args[i], stack, stack_base + 1)
    
            # XXX: Dynamic scope hack -- we need to visit and force eval every time we resume (in case we
            #      are now running on a different machine with a different implementation for the function).
            #      This is okay because dynamic scope functions are all outwith Skywriting, so there will
            #      not be any implementation-specific junk further down the stack.
            function = self.visit_and_force_eval(node.function, stack[0:stack_base+1], stack_base + 1)
            ret = function.call(resume_record.args, stack, stack_base + 1, self.context)
        
            stack.pop()
            return ret
        
        except:
            raise
        
    def visit_FunctionDeclaration(self, node, stack, stack_base):
        ret = UserDefinedFunction(self.context, node)
        return ret
    
    def visit_GreaterThan(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left > rexpr

        except:
            raise
    
    def visit_GreaterThanOrEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left >= rexpr

        except:
            raise

    def visit_Identifier(self, node, stack, stack_base):
        return self.context.value_of(node.identifier)

    def visit_KeyValuePair(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            # We store the key in resume_record.left
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.key_expr, stack, stack_base + 1)
            
            value = self.visit_and_force_eval(node.value_expr, stack, stack_base + 1)
            
        except:
            raise

        stack.pop()
        return resume_record.left, value
    
    def visit_LambdaExpression(self, node, stack, stack_base):
        return UserDefinedLambda(self.context, node)
    
    def visit_LessThan(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left < rexpr

        except:
            raise
    
    def visit_LessThanOrEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = LessThanOrEqualRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left <= rexpr

        except:
            raise
    
    def visit_List(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = ListRR(len(node.contents))
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        
        try:
            
            for i in range(len(node.contents)):
                if resume_record.items[i] is None:
                    resume_record.items[i] = self.visit_and_force_eval(node.contents[i], stack, stack_base + 1)
    
            stack.pop()
            return resume_record.items
        
        except:
            raise
        
    def visit_ListIndex(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = ListIndexRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            if resume_record.list is None:
                resume_record.list = self.visit_and_force_eval(node.list_expr, stack, stack_base + 1)
                
            index = self.visit_and_force_eval(node.index, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.list[index]
        
        except:
            raise
    
    def visit_Minus(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left - rexpr

        except:
            raise
    
    def visit_Not(self, node, stack, stack_base):
        return not self.visit_and_force_eval(node.expr, stack, stack_base)
    
    def visit_UnaryMinus(self, node, stack, stack_base):
        return -self.visit_and_force_eval(node.expr, stack, stack_base)

    def visit_NotEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left != rexpr

        except:
            raise
    
    def visit_Or(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left or rexpr

        except:
            raise
    
    def visit_Plus(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = PlusRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]

        try:
            if resume_record.left is None:
                resume_record.left = self.visit_and_force_eval(node.lexpr, stack, stack_base + 1)

            rexpr = self.visit_and_force_eval(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return resume_record.left + rexpr

        except:
            raise
    
class UserDefinedLambda:
    
    def __init__(self, declaration_context, lambda_ast):
        self.lambda_ast = lambda_ast
        self.captured_bindings = {}
        
        body_bindings = FunctionDeclarationBindingVisitor()
        body_bindings.visit(lambda_ast.expr)
        
        for identifier in body_bindings.rvalue_object_identifiers:
            if declaration_context.has_binding_for(identifier) and not declaration_context.is_dynamic(identifier):
                self.captured_bindings[identifier] = declaration_context.value_of(identifier)
                
    def call(self, args_list, stack, stack_base, context):
        context.enter_context(self.captured_bindings)
        for (formal_param, actual_param) in zip(self.lambda_ast.variables, args_list):
            context.bind_identifier(formal_param, actual_param)
        ret = ExpressionEvaluatorVisitor(context).visit(self.lambda_ast.expr, stack, stack_base)
        context.exit_context()
        return ret

class UserDefinedFunction:
    
    def __init__(self, declaration_context, function_ast):
        self.function_ast = function_ast
        self.captured_bindings = {}
        
        body_bindings = FunctionDeclarationBindingVisitor()
        body_bindings.visit_statement_list(function_ast.body)
        

        formal_params = set(function_ast.formal_params)
        
        for variable in body_bindings.lvalue_object_identifiers:
            if declaration_context.has_binding_for(variable):
                # Free variables are read-only.
                raise
            elif variable in formal_params:
                # Formal parameters are read-only.
                raise
            
        #self.execution_context = execution_context
        
        for identifier in body_bindings.rvalue_object_identifiers:
            if declaration_context.has_binding_for(identifier):
                if not declaration_context.is_dynamic(identifier):
                    self.captured_bindings[identifier] = declaration_context.value_of(identifier)
            elif function_ast.name is not None and identifier == function_ast.name.identifier:
                self.captured_bindings[identifier] = self
                #self.execution_context.bind_identifier(object, declaration_context.value_of(object))
        
    def __repr__(self):
        return 'UserDefinedFunction(name=%s)' % self.function_ast.name 
        
    def call(self, args_list, stack, stack_base, context):
        context.enter_context(self.captured_bindings)
        #self.execution_context.enter_scope()
        for (formal_param, actual_param) in zip(self.function_ast.formal_params, args_list):
            context.bind_identifier(formal_param, actual_param)
            
        # Belt-and-braces approach to protect formal parameters (not strictly necessary).
        # TODO: runtime protection in case lists, etc. get aliased.
        context.enter_scope()
        ret = StatementExecutorVisitor(context).visit_statement_list(self.function_ast.body, stack, stack_base)
        context.exit_scope()

        context.exit_context()
        return ret
    
# TODO: could do better than this by passing over the whole script at the start. But
# let's take a simple approach for now.
class FunctionDeclarationBindingVisitor(Visitor):
    
    def __init__(self):
        self.lvalue_object_identifiers = set()
        self.rvalue_object_identifiers = set()
        
    def visit_statement_list(self, statements):
        for statement in statements:
            self.visit(statement)
            
    def visit_Assignment(self, node):
        self.visit(node.lvalue)
        self.visit(node.rvalue)
        
    def visit_Break(self, node):
        pass
    
    def visit_Continue(self, node):
        pass
    
    def visit_If(self, node):
        self.visit(node.condition)
        self.visit_statement_list(node.true_body)
        if node.false_body is not None:
            self.visit_statement_list(node.false_body)
    
    def visit_PlusAssignment(self, node):
        self.visit(node.lvalue)
        self.visit(node.rvalue)
    
    def visit_Return(self, node):
        if node.expr is not None:
            self.visit(node.expr)
    
    def visit_Do(self, node):
        self.visit_statement_list(node.body)
        self.visit(node.condition)
    
    def visit_For(self, node):
        self.visit(node.indexer)
        self.visit(node.iterator)
        self.visit_statement_list(node.body)
        
    def visit_While(self, node):
        self.visit(node.condition)
        self.visit_statement_list(node.body)
        
    def visit_IdentifierLValue(self, node):
        self.lvalue_object_identifiers.add(node.identifier)
  
    def visit_IndexedLValue(self, node):
        self.visit(node.base_lvalue)
        
    def visit_FieldLValue(self, node):
        self.visit(node.base_lvalue)

    def visit_Constant(self, node):
        pass

    def visit_Dereference(self, node):
        self.visit(node.reference)
        
    def visit_Dict(self, node):
        for item in node.items:
            self.visit(item)
        
    def visit_FieldReference(self, node):
        self.visit(node.object)

    def visit_FunctionCall(self, node):
        self.visit(node.function)
        for arg in node.args:
            self.visit(arg)
        
    def visit_FunctionDeclaration(self, node):
        self.visit_statement_list(node.body)
        
    def visit_Identifier(self, node):
        self.rvalue_object_identifiers.add(node.identifier)
        
    def visit_KeyValuePair(self, node):
        self.visit(node.key_expr)
        self.visit(node.value_expr)
        
    def visit_LambdaExpression(self, node):
        self.visit(node.expr)
        
    def visit_List(self, node):
        for elem in node.contents:
            self.visit(elem)
        
    def visit_ListIndex(self, node):
        self.visit(node.list_expr)
        self.visit(node.index)
        
    def visit_Not(self, node):
        self.visit(node.expr)

    def visit_UnaryMinus(self, node):
        self.visit(node.expr)
        
    def visit_BinaryExpression(self, node):
        self.visit(node.lexpr)
        self.visit(node.rexpr)
        
    visit_And = visit_BinaryExpression
    visit_Equal = visit_BinaryExpression
    visit_GreaterThan = visit_BinaryExpression
    visit_GreaterThanOrEqual = visit_BinaryExpression
    visit_LessThan = visit_BinaryExpression
    visit_LessThanOrEqual = visit_BinaryExpression
    visit_Minus = visit_BinaryExpression
    visit_NotEqual = visit_BinaryExpression
    visit_Or = visit_BinaryExpression
    visit_Plus = visit_BinaryExpression
