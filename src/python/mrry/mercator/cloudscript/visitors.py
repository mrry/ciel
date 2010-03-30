'''
Created on 23 Feb 2010

@author: dgm36
'''
from mrry.mercator.cloudscript.interpreter.resume import BinaryExpressionRR,\
    FunctionCallRR, ListRR, DictRR, StatementListRR, DoRR, IfRR, WhileRR, ForRR,\
    ListIndexRR


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
        
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node, stack, stack_base)
        
    def visit_statement_list(self, statements, stack, stack_base):
        if stack_base == len(stack):
            resume_record = StatementListRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            for i in range(resume_record.current_statement_index, len(statements)):
                resume_record.current_statement_index = i
            
                print i
                ret = self.visit(statements[i], stack, stack_base + 1)
        
                if ret is not None:
                    break
                
        except:
            raise
            
        stack.pop()
        return ret
    
    def visit_Assignment(self, node, stack, stack_base):
        lvalue = node.lvalue
        
        try:
            rvalue = ExpressionEvaluatorVisitor(self.context).visit(node.rvalue, stack, stack_base)
        except:
            raise
        
        self.context.update_value(lvalue, rvalue)
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

                condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition, stack, stack_base + 1)

                if not condition:
                    ret = None
                    break

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
                resume_record.condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition, stack, stack_base + 1)
            
            if resume_record.condition:
                ret = self.visit_statement_list(node.true_body, stack, stack_base + 1)
            elif node.false_body is not None:
                ret = self.visit_statement_list(node.false_body, stack, stack_base + 1)
            
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
                resume_record.iterator = ExpressionEvaluatorVisitor(self.context).visit(node.iterator, stack, stack_base + 1)

            indexer_lvalue = node.indexer                
            for i in range(resume_record.i, len(resume_record.iterator)):
                resume_record.i = i
                self.context.update_value(indexer_lvalue, resume_record.iterator[i])
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

    def visit_Return(self, node, stack, stack_base):
        if node.expr is not None:
            return ExpressionEvaluatorVisitor(self.context).visit(node.expr, stack, stack_base)
        else:
            return None

    def visit_While(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = WhileRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        try:
            
            while True:
                
                if not resume_record.done_condition:
                    condition = ExpressionEvaluatorVisitor(self.context).visit(node.condition, stack, stack_base + 1)
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
        
        except:
            raise
        
        stack.pop()
        return ret

    def visit_Script(self, node, stack, stack_base):
        self.visit_statement_list(node.body, stack, stack_base)

class ExecutionInterruption(Exception):
    
    def __init__(self, resume_chain=[]):
        self.resume_chain = resume_chain

class ExpressionEvaluatorVisitor:
    
    def __init__(self, context):
        self.context = context
    
    def visit(self, node, stack, stack_base):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node, stack, stack_base)
    
    def visit_And(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr and rexpr

        except:
            raise
    
    def visit_Constant(self, node, stack, stack_base):
        return node.value
    
    def visit_Dereference(self, node, stack, stack_base):
        # FIXME! Need to add to a list of pending data, and return a wrapper object that will cause an
        # execution fault if the user attempts to use the dereferenced value.
        reference = self.visit(node.reference)
        star_function = self.context.value_of('__star__')
        return star_function.call([reference])
    
    def visit_Dict(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = DictRR(len(node.items))
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
        
        try:    
            ret = {}
            for i in len(node.items):
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
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr == rexpr

        except:
            raise
        
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
        
            function = self.visit(node.function, stack, stack_base + 1)
            
            ret = function.call(resume_record.args, stack, stack_base + 1)
        
            stack.pop()
            return ret
        
        except:
            raise
        
    
    def visit_FunctionDeclaration(self, node, stack, stack_base):
        return UserDefinedFunction(self.context, self.context.fresh_context(), node)
    
    def visit_GreaterThan(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr == rexpr

        except:
            raise
    
    def visit_GreaterThanOrEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr == rexpr

        except:
            raise

    def visit_Identifier(self, node, stack, stack_base):
        return self.context.value_of(node.identifier)

    def visit_KeyValuePair(self, node, stack, stack_base):
        key = self.visit(node.key_expr)
        value = self.visit(node.value_expr)
        return key, value
    
    def visit_LambdaExpression(self, node, stack, stack_base):
        return UserDefinedLambda(self.context, self.context.fresh_context(), node)
    
    def visit_LessThan(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr < rexpr

        except:
            raise
    
    def visit_LessThanOrEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr <= rexpr

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
                    resume_record.items[i] = self.visit(node.contents[i], stack, stack_base + 1)
    
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
                resume_record.list = self.visit(node.list_expr, stack, stack_base + 1)
                
            index = self.visit(node.index, stack, stack_base + 1)
            
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
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr - rexpr

        except:
            raise
    
    def visit_Not(self, node, stack, stack_base):
        return not self.visit(node.expr, stack, stack_base)
    
    def visit_NotEqual(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr != rexpr

        except:
            raise
    
    def visit_Or(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr or rexpr

        except:
            raise
    
    def visit_Plus(self, node, stack, stack_base):
        if stack_base == len(stack):
            resume_record = BinaryExpressionRR()
            stack.append(resume_record)
        else:
            resume_record = stack[stack_base]
            
        if resume_record.left is not None:
            lexpr = resume_record.left

        try:
            if lexpr is None:
                lexpr = self.visit(node.lexpr, stack, stack_base + 1)
                resume_record.left = lexpr

            rexpr = self.visit(node.rexpr, stack, stack_base + 1)
            
            stack.pop()
            return lexpr + rexpr

        except:
            raise
    
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
                
    def call(self, args_list, stack, stack_base):
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
        
    def call(self, args_list, stack, stack_base):
        self.execution_context.enter_subcontext()
        for (formal_param, actual_param) in zip(self.function_ast.formal_params, args_list):
            self.execution_context.bind_identifier(formal_param, actual_param)
            
        # Belt-and-braces approach to protect formal parameters (not strictly necessary).
        # TODO: runtime protection in case lists, etc. get aliased.
        self.execution_context.enter_subcontext()
            
        ret = StatementExecutorVisitor(self.execution_context).visit_statement_list(self.function_ast.body, stack, stack_base)
        
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