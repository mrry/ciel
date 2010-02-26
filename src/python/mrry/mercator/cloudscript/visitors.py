'''
Created on 23 Feb 2010

@author: dgm36
'''


class Visitor:
    
    def visit(self, node):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node)

class StatementResult:
    pass
RESULT_BREAK = StatementResult()
RESULT_CONTINUE = StatementResult()

class StatementExecutorVisitor(Visitor):
    
    def __init__(self, context):
        self.context = context
        
    def visit_statement_list(self, statements):
        for statement in statements:
            print statement
            ret = self.visit(statement)
            if ret is not None:
                return ret
        return None
    
    def visit_Assignment(self, node):
        lvalue = node.lvalue
        rvalue = ExpressionEvaluatorVisitor(self.context).visit(node.rvalue)
        self.context.update_value(lvalue, rvalue)
        return None
    
    def visit_Break(self, node):
        return RESULT_BREAK
    
    def visit_Continue(self, node):
        return RESULT_CONTINUE
    
    def visit_Do(self, node):
        while True:
            
            ret = self.visit_statement_list(node.body)

            if ret is not None:
                if ret is RESULT_BREAK:
                    ret = None
                    break
                elif ret is RESULT_CONTINUE:
                    continue
                else:
                    break
                
            condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition)

            if not condition:
                break
                
        return ret
    
    def visit_If(self, node):
        condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition)
        if condition:
            return self.visit_statement_list(node.true_body)
        elif node.false_body is not None:
            return self.visit_statement_list(node.false_body)

    def visit_For(self, node):
        iterator = ExpressionEvaluatorVisitor(self.context).visit(node.iterator)
        indexer_lvalue = node.indexer
        for indexer in iterator:
            self.context.update_value(indexer_lvalue, indexer)
            
            ret = self.visit_statement_list(node.body)

            if ret is not None:
                if ret is RESULT_BREAK:
                    ret = None
                    break
                elif ret is RESULT_CONTINUE:
                    continue
                else:
                    break
            
        self.context.remove_binding(indexer_lvalue.identifier)
        return ret

    def visit_Return(self, node):
        if node.expr is not None:
            return ExpressionEvaluatorVisitor(self.context).visit(node.expr)
        else:
            return None

    def visit_While(self, node):
        while True:
            condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition)
            if not condition:
                break
            
            ret = self.visit_statement_list(node.body)

            if ret is not None:
                if ret is RESULT_BREAK:
                    break
                elif ret is RESULT_CONTINUE:
                    continue
                else:
                    break

        return None

    def visit_Script(self, node):
        self.visit_statement_list(node.body)

class ExpressionEvaluatorVisitor(Visitor):
    
    def __init__(self, context):
        self.context = context
    
    def visit_And(self, node):
        return self.visit(node.lexpr) and self.visit(node.rexpr)
    
    def visit_Constant(self, node):
        return node.value
    
    def visit_Dict(self, node):
        ret = {}
        for item in node.items:
            key, value = self.visit(item)
            ret[key] = value
        return ret
    
    def visit_Equal(self, node):
        return self.visit(node.lexpr) == self.visit(node.rexpr)
    
    def visit_FunctionCall(self, node):
        function = self.visit(node.function)
        args = [self.visit(arg) for arg in node.args]
        print self.context.bindings
        return function.call(args)
    
    def visit_FunctionDeclaration(self, node):
        return UserDefinedFunction(self.context, self.context.fresh_context(), node)
    
    def visit_GreaterThan(self, node):
        return self.visit(node.lexpr) > self.visit(node.rexpr)
    
    def visit_GreaterThanOrEqual(self, node):
        return self.visit(node.lexpr) >= self.visit(node.rexpr)
    
    def visit_Identifier(self, node):
        return self.context.value_of(node.identifier)

    def visit_KeyValuePair(self, node):
        key = self.visit(node.key_expr)
        value = self.visit(node.value_expr)
        return key, value
    
    def visit_LambdaExpression(self, node):
        return UserDefinedLambda(self.context, self.context.fresh_context(), node)
    
    def visit_LessThan(self, node):
        return self.visit(node.lexpr) < self.visit(node.rexpr)
    
    def visit_LessThanOrEqual(self, node):
        return self.visit(node.lexpr) <= self.visit(node.rexpr)
    
    def visit_List(self, node):
        return [self.visit(elem) for elem in node.contents]
    
    def visit_ListIndex(self, node):
        list = self.visit(node.list_expr)
        index = self.visit(node.index)
        return list[index]
    
    def visit_Minus(self, node):
        return self.visit(node.lexpr) - self.visit(node.rexpr)
    
    def visit_Not(self, node):
        return not self.visit(node.expr)
    
    def visit_NotEqual(self, node):
        return self.visit(node.lexpr) != self.visit(node.rexpr)
    
    def visit_Or(self, node):
        return self.visit(node.lexpr) or self.visit(node.rexpr)
    
    def visit_Plus(self, node):
        return self.visit(node.lexpr) + self.visit(node.rexpr)
    
class CloudScriptFunction:
    
    def call(self, args_list):
        pass
    
class UserDefinedLambda:
    
    def __init__(self, declaration_context, execution_context, lambda_ast):
        self.lambda_ast = lambda_ast
        
        body_bindings = FunctionDeclarationBindingVisitor()
        body_bindings.visit(lambda_ast.expr)
        
        self.execution_context = execution_context
        
        for object in body_bindings.rvalue_object_identifiers:
            if declaration_context.has_binding_for(object):
                self.execution_context.bind_identifier(object, declaration_context.value_of(object))
                
    def call(self, args_list):
        self.execution_context.enter_subcontext()
        for (formal_param, actual_param) in zip(self.lambda_ast.variables, args_list):
            self.execution_context.bind_identifier(formal_param, actual_param)
        ret = ExpressionEvaluatorVisitor(self.execution_context).visit(self.lambda_ast.expr)
        self.execution_context.exit_subcontext()
        return ret
            
        
    
class UserDefinedFunction:
    
    def __init__(self, declaration_context, execution_context, function_ast):
        
        self.function_ast = function_ast
        
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
            
        self.execution_context = execution_context
        
        for object in body_bindings.rvalue_object_identifiers:
            if declaration_context.has_binding_for(object):
                self.execution_context.bind_identifier(object, declaration_context.value_of(object))
        
    def call(self, args_list):
        self.execution_context.enter_subcontext()
        for (formal_param, actual_param) in zip(self.function_ast.formal_params, args_list):
            self.execution_context.bind_identifier(formal_param, actual_param)
            
        # Belt-and-braces approach to protect formal parameters (not strictly necessary).
        # TODO: runtime protection in case lists, etc. get aliased.
        self.execution_context.enter_subcontext()
            
        ret = StatementExecutorVisitor(self.execution_context).visit_statement_list(self.function_ast.body)
        
        self.execution_context.exit_subcontext()
        self.execution_context.exit_subcontext()
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
        
    def visit_BinaryExpression(self, node):
        self.visit(node.lexpr)
        self.visit(node.rexpr)
        
    visit_And = visit_BinaryExpression
    visit_Equals = visit_BinaryExpression
    visit_GreaterThan = visit_BinaryExpression
    visit_GreaterThanOrEqual = visit_BinaryExpression
    visit_LessThan = visit_BinaryExpression
    visit_LessThanOrEqual = visit_BinaryExpression
    visit_Minus = visit_BinaryExpression
    visit_NotEqual = visit_BinaryExpression
    visit_Or = visit_BinaryExpression
    visit_Plus = visit_BinaryExpression