'''
Created on 22 Feb 2010

@author: dgm36
'''

class ASTNode:
    
    def __init__(self):
        pass
    
class Statement(ASTNode):
    
    def execute(self, context):
        pass

class StatementResult:
    pass
RESULT_BREAK = StatementResult()
RESULT_CONTINUE = StatementResult()

def execute_statement_list(context, statement_list):
    pass

class Assignment(Statement):
    
    def __init__(self, lvalue, rvalue):
        self.lvalue = lvalue
        self.rvalue = rvalue
        
class Break(Statement):
    
    def __init__(self):
        pass
    
    def execute(self, context):
        return RESULT_BREAK

class Continue(Statement):
    
    def __init__(self):
        pass
    
    def execute(self, context):
        return RESULT_CONTINUE
    
class Do(Statement):
    
    def __init__(self, body, condition):
        self.body = body
        self.condition = condition

class For(Statement):
    
    def __init__(self, indexer, iterator, body):
        self.indexer = indexer
        self.iterator = iterator
        self.body = body
        
class If(Statement):
    
    def __init__(self, condition, true_body, false_body=None):
        self.condition = condition
        self.true_body = true_body
        self.false_body = false_body
 
class PlusAssignment(Statement):
    
    def __init__(self, lvalue, rvalue):
        self.lvalue = lvalue
        self.rvalue = rvalue
    
class Return(Statement):
    
    def __init__(self, expr=None):
        self.expr = expr
        
class Script(Statement):
    
    def __init__(self, body):
        self.body = body
        
class While(Statement):
    
    def __init__(self, condition, body):
        self.condition = condition
        self.body = body
        
class LValue(ASTNode):
    
    def base_identifier(self):
        pass
    
class IdentifierLValue(LValue):
    
    def __init__(self, identifier):
        self.identifier = identifier
        
    def base_identifier(self):
        return self.identifier
    
class FieldLValue(LValue):
    
    def __init__(self, base_lvalue, field_name):
        self.base_lvalue = base_lvalue
        self.field_name = field_name
        
    def base_identifier(self):
        return self.base_lvalue.base_identifier()
    
class IndexedLValue(LValue):
    
    def __init__(self, base_lvalue, index):
        self.base_lvalue = base_lvalue
        self.index = index
      
    def base_identifier(self):
        return self.base_lvalue.base_identifier()
        
class Expression(ASTNode):
    
    def eval(self, context):
        pass
    
class Constant(Expression):
    
    def __init__(self, value):
        self.value = value
    
class Dereference(Expression):
    
    def __init__(self, reference):
        self.reference = reference
    
class FieldReference(Expression):
    
    def __init__(self, object, field):
        self.object = object
        self.field = field    

class FunctionCall(Expression):
    
    def __init__(self, function, args=[]):
        self.function = function
        self.args = args
        
class FunctionDeclaration(Expression):
    
    def __init__(self, formal_params, body):
        self.formal_params = formal_params
        self.body = body
    
class Identifier(Expression):
    
    def __init__(self, identifier):
        self.identifier = identifier
    
class LambdaExpression(Expression):
    
    def __init__(self, expr, variables=[]):    
        self.variables = variables
        self.expr = expr

class List(Expression):
    
    def __init__(self, contents=[]):
        self.contents = contents

class ListIndex(Expression):
    
    def __init__(self, list_expr, index):
        self.list_expr = list_expr
        self.index = index
    
class Not(Expression):
    
    def __init__(self, expr):
        self.expr = expr
        
class BinaryExpression(Expression):
    
    def __init__(self, lexpr, rexpr):
        self.lexpr = lexpr
        self.rexpr = rexpr

class And(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)

class Equal(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)

class GreaterThan(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)

class GreaterThanOrEqual(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)

class LessThan(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)
        
class LessThanOrEqual(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)

class Minus(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)
       
class NotEqual(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)       
 
class Or(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)        
        
class Plus(BinaryExpression):
    
    def __init__(self, lexpr, rexpr):
        BinaryExpression.__init__(self, lexpr, rexpr)