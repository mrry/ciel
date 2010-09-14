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
        
    def __repr__(self):
        return 'Assignment(lvalue=%s, rvalue=%s)' % (self.lvalue, self.rvalue)
        
class Break(Statement):
    
    def __init__(self):
        pass
    
    def execute(self, context):
        return RESULT_BREAK
    
    def __repr__(self):
        return 'Break()'

class Continue(Statement):
    
    def __init__(self):
        pass
    
    def execute(self, context):
        return RESULT_CONTINUE
    
    def __repr__(self):
        return 'Continue()'
    
class Do(Statement):
    
    def __init__(self, body, condition):
        self.body = body
        self.condition = condition

    def __repr__(self):
        return 'Do(body=%s, condition=%s)' % (repr(self.body), repr(self.condition))

class For(Statement):
    
    def __init__(self, indexer, iterator, body):
        self.indexer = indexer
        self.iterator = iterator
        self.body = body
        
    def __repr__(self):
        return 'For(indexer=%s, iterator=%s, body=%s)' % (repr(self.indexer), repr(self.iterator), repr(self.body))
        
class If(Statement):
    
    def __init__(self, condition, true_body, false_body=None):
        self.condition = condition
        self.true_body = true_body
        self.false_body = false_body
        
    def __repr__(self):
        return 'If(condition=%s, true_body=%s, false_body=%s)' % (repr(self.condition), repr(self.true_body), repr(self.false_body))

class Include(Statement):
    
    def __init__(self, target_expr, included_script=None):
        self.target_expr = target_expr
        self.included_script = None
 
    def __repr__(self):
        return 'Include(target_expr=%s, included_script=%s)' % (repr(self.target_expr), repr(self.included_script))
 
class PlusAssignment(Statement):
    
    def __init__(self, lvalue, rvalue):
        self.lvalue = lvalue
        self.rvalue = rvalue
    
class Return(Statement):
    
    def __init__(self, expr=None):
        self.expr = expr

    def __repr__(self):
        return 'Return(%s)' % repr(self.expr)
        
class Script(Statement):
    
    def __init__(self, body):
        self.body = body
        
    def __repr__(self):
        return 'Script(%s)' % repr(self.body)
        
class While(Statement):
    
    def __init__(self, condition, body):
        self.condition = condition
        self.body = body
        
    def __repr__(self):
        return 'While(condition=%s, body=%s)' % (repr(self.condition), repr(self.body))
        
class NamedFunctionDeclaration(Statement):
    
    def __init__(self, name, formal_params, body):
        self.name = name
        self.formal_params = formal_params
        self.body = body
        
    def __repr__(self):
        return 'NamedFunctionDeclaration(name=%s, formal_params=%s, body=%s)' % (self.name, self.formal_params, self.body)
        
class LValue(ASTNode):
    
    def base_identifier(self):
        pass
    
class IdentifierLValue(LValue):
    
    def __init__(self, identifier):
        self.identifier = identifier
        
    def base_identifier(self):
        return self.identifier
    
    def __repr__(self):
        return 'IdentifierLValue(%s)' % repr(self.identifier)
    
class FieldLValue(LValue):
    
    def __init__(self, base_lvalue, field_name):
        self.base_lvalue = base_lvalue
        self.field_name = field_name
        
    def base_identifier(self):
        return self.base_lvalue.base_identifier()
    
    def __repr__(self):
        return 'FieldLValue(base_lvalue=%s, field_name=%s)' % (self.base_lvalue, self.field_name) 
    
class IndexedLValue(LValue):
    
    def __init__(self, base_lvalue, index):
        self.base_lvalue = base_lvalue
        self.index = index
      
    def base_identifier(self):
        return self.base_lvalue.base_identifier()

    def __repr__(self):
        return 'FieldLValue(base_lvalue=%s, index=%s)' % (self.base_lvalue, self.index) 
        
class Expression(ASTNode):
    
    def eval(self, context):
        pass
    
class Constant(Expression):
    
    def __init__(self, value):
        self.value = value
        
    def __repr__(self):
        return 'Constant(%s)' % repr(self.value)
    
class Dereference(Expression):
    
    def __init__(self, reference):
        self.reference = reference

    def __repr__(self):
        return 'Dereference(%s)' % repr(self.reference)

class Dict(Expression):
    
    def __init__(self, items=[]):
        self.items = items

    def __repr__(self):
        return 'Dict(%s)' % repr(self.items)
    
class FieldReference(Expression):
    
    def __init__(self, object, field):
        self.object = object
        self.field = field    

    def __repr__(self):
        return 'FieldReference(object=%s, field=%s)' % (self.object, self.field)

# Pseudo-AST node used when we are spawning a function and have already visited the
# function body and args.
class SpawnedFunction(Expression):

    def __init__(self, function, args):
        self.function = function
        self.args = args
        
    def __repr__(self):
        return 'SpawnedFunction(function=%s, args=%s)' % (repr(self.function), repr(self.args))

class FunctionCall(Expression):
    
    def __init__(self, function, args=[]):
        self.function = function
        self.args = args

    def __repr__(self):
        return 'FunctionCall(function=%s, args=%s)' % (repr(self.function), repr(self.args))
     
class FunctionDeclaration(Expression):
    
    def __init__(self, formal_params, body):
        self.formal_params = formal_params
        self.body = body
        self.name = None
    
    def __repr__(self):
        return 'FunctionDeclaration(formal_params=%s, body=%s)' % (repr(self.formal_params), repr(self.body))
    
class Identifier(Expression):
    
    def __init__(self, identifier):
        self.identifier = identifier
        
    def __repr__(self):
        return 'Identifier(%s)' % (self.identifier, )
    
class KeyValuePair(Expression):
    
    def __init__(self, key_expr, value_expr):
        self.key_expr = key_expr
        self.value_expr = value_expr

    def __repr__(self):
        return 'KeyValuePair(key_expr=%s, value_expr=%s)' % (repr(self.key_expr), repr(self.value_expr))
    
class LambdaExpression(Expression):
    
    def __init__(self, expr, variables=[]):    
        self.variables = variables
        self.expr = expr
        self.name = None

    def __repr__(self):
        return 'LambdaExpression(variables=%s, expr=%s)' % (repr(self.variables), repr(self.expr))

class List(Expression):
    
    def __init__(self, contents=[]):
        self.contents = contents
        
    def __repr__(self):
        return 'List(%s)' % self.contents

class ListIndex(Expression):
    
    def __init__(self, list_expr, index):
        self.list_expr = list_expr
        self.index = index

    def __repr__(self):
        return 'ListIndex(list_expr=%s, index=%s)' % (repr(self.list_expr), repr(self.index))
    
class Not(Expression):
    
    def __init__(self, expr):
        self.expr = expr
    
    def __repr__(self):
        return 'Not(%s)' % repr(self.expr)
        
class UnaryMinus(Expression):

    def __init__(self, expr):
        self.expr = expr

    def __repr__(self):
        return 'UnaryMinus(%s)' % repr(self.expr)

class BinaryExpression(Expression):
    
    def __init__(self, lexpr, rexpr):
        self.lexpr = lexpr
        self.rexpr = rexpr
        
    def __repr__(self):
        return '%s(lexpr=%s, rexpr=%s)' % (repr(self.__class__.__name__), repr(self.lexpr), repr(self.rexpr))

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
