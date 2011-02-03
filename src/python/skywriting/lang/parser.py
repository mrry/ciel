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
from skywriting.lang.lexer import CloudScriptLexer
import ply.yacc
from skywriting.lang import ast

class SyntaxException:
    def __init__(self, token):
        self.token = token

    def __repr__(self):
        return 'SyntaxException(%s)' % repr(self.token)

class CloudScriptParser:
    
    def __init__(self):
        self.lexer = CloudScriptLexer()
        self.lexer.build()
        
        self.tokens = self.lexer.tokens
        
        self.parser = ply.yacc.yacc(module=self, start='script_file')

    precedence = (
        ('left', 'OR'),
        ('left', 'AND'),
        ('left', 'EQ', 'NE'),
        ('left', 'GT', 'GEQ', 'LT', 'LEQ'),
        ('left', 'PLUS', 'MINUS'),
    )
 
    def parse(self, text):
        return self.parser.parse(text)
    
    def p_error(self, p):
        print "Syntax error on line %d: %s" % (p.lineno, repr(p.value))
        raise SyntaxException(p)

    def p_script_file(self, p):
        """ script_file : statement_list
        """
        p[0] = ast.Script(p[1])
        
    def p_statement_list(self, p):
        """ statement_list : statement
                           | statement_list statement
        """
        if len(p) == 2:
            p[0] = [p[1]] if p[1] else []
        else:
            p[0] = p[1] + ([p[2]] if p[2] else [])
    
    def p_statement(self, p):
        """ statement : assignment_statement
                      | loop_statement
                      | if_statement
                      | break_statement
                      | continue_statement
                      | return_statement
                      | include_statement
                      | compound_statement
                      | named_function_decl
        """
        p[0] = p[1]
        
    def p_assignment_statement_1(self, p):
        """ assignment_statement : lvalue ASSIGN expression SEMICOLON
        """
        p[0] = ast.Assignment(p[1], p[3])
    
    def p_assignment_statement_2(self, p):
        """ assignment_statement : lvalue ASSIGN_PLUS expression SEMICOLON
        """
        p[0] = ast.PlusAssignment(p[1], p[3])
    
    def p_break_statement(self, p):
        """ break_statement : BREAK SEMICOLON
        """
        p[0] = ast.Break()
        
    def p_continue_statement(self, p):
        """ continue_statement : CONTINUE SEMICOLON
        """
        p[0] = ast.Continue()
        
    def p_include_statement(self, p):
        """ include_statement : INCLUDE expression SEMICOLON
        """
        p[0] = ast.Include(p[2])
        
    def p_return_statement_1(self, p):
        """ return_statement : RETURN SEMICOLON
        """
        p[0] = ast.Return()
        
    def p_return_statement_2(self, p):
        """ return_statement : RETURN expression SEMICOLON
        """
        p[0] = ast.Return(p[2])
        
    def p_loop_statement(self, p):
        """ loop_statement : for_loop_statement
                           | do_loop_statement
                           | while_loop_statement
        """
        p[0] = p[1]
        
    def p_for_loop_statement(self, p):
        """ for_loop_statement : FOR LPAREN ID IN expression RPAREN compound_statement
        """
        p[0] = ast.For(ast.IdentifierLValue(p[3]), p[5], p[7])
    
    def p_do_loop_statement(self, p):
        """ do_loop_statement : DO compound_statement WHILE LPAREN expression RPAREN SEMICOLON
        """
        p[0] = ast.Do(p[2], p[5])
    
    def p_while_loop_statement(self, p):
        """ while_loop_statement : WHILE LPAREN expression RPAREN compound_statement
        """
        p[0] = ast.While(p[3], p[5])
    
    def p_compound_statement(self, p):
        """ compound_statement : LBRACE statement_list RBRACE
        """
        p[0] = p[2]
        
    def p_if_statement_1(self, p):
        """ if_statement : IF LPAREN expression RPAREN compound_statement
        """
        p[0] = ast.If(p[3], p[5])
    
    def p_if_statement_2(self, p):
        """ if_statement : IF LPAREN expression RPAREN compound_statement ELSE compound_statement
        """
        p[0] = ast.If(p[3], p[5], p[7])
        
    def p_lvalue_1(self, p):
        """ lvalue : ID
        """
        p[0] = ast.IdentifierLValue(p[1])
        
    def p_lvalue_2(self, p):
        """ lvalue : lvalue DOT ID
        """
        p[0] = ast.FieldLValue(p[1])
        
    def p_lvalue_3(self, p):
        """ lvalue : lvalue LBRACKET expression RBRACKET
        """
        p[0] = ast.IndexedLValue(p[1], p[3])
        
    def p_expression(self, p):
        """ expression : binary_expression
        """
        p[0] = p[1]

    def p_binary_expression_1(self, p):
        """ binary_expression : unary_expression
        """
        p[0] = p[1]

    def p_binary_expression_2(self, p):
        """ binary_expression : binary_expression PLUS binary_expression
        """
        p[0] = ast.Plus(p[1], p[3])
        
    def p_binary_expression_3(self, p):
        """ binary_expression : binary_expression MINUS binary_expression
        """
        p[0] = ast.Minus(p[1], p[3])
        
    def p_binary_expression_4(self, p):
        """ binary_expression : binary_expression OR binary_expression
        """
        p[0] = ast.Or(p[1], p[3])
        
    def p_binary_expression_5(self, p):
        """ binary_expression : binary_expression AND binary_expression
        """
        p[0] = ast.And(p[1], p[3])
        
    def p_binary_expression_6(self, p):
        """ binary_expression : binary_expression LT binary_expression
        """
        p[0] = ast.LessThan(p[1], p[3])
        
    def p_binary_expression_7(self, p):
        """ binary_expression : binary_expression GT binary_expression
        """
        p[0] = ast.GreaterThan(p[1], p[3])
        
    def p_binary_expression_8(self, p):
        """ binary_expression : binary_expression LEQ binary_expression
        """
        p[0] = ast.LessThanOrEqual(p[1], p[3])
        
    def p_binary_expression_9(self, p):
        """ binary_expression : binary_expression GEQ binary_expression
        """
        p[0] = ast.GreaterThanOrEqual(p[1], p[3])
        
    def p_binary_expression_10(self, p):
        """ binary_expression : binary_expression EQ binary_expression
        """
        p[0] = ast.Equal(p[1], p[3])
                              
    def p_binary_expression_11(self, p):
        """ binary_expression : binary_expression NE binary_expression
        """
        p[0] = ast.NotEqual(p[1], p[3])
        
    def p_unary_expression_1(self, p):
        """ unary_expression : postfix_expression
        """
        p[0] = p[1]
    
    def p_unary_expression_2(self, p):
        """ unary_expression : STAR unary_expression
        """
        p[0] = ast.Dereference(p[2])

    def p_unary_expression_3(self, p):
        """ unary_expression : NOT binary_expression
        """
        p[0] = ast.Not(p[2])

    def p_unary_expression_4(self, p):
        """ unary_expression : MINUS binary_expression
        """
        p[0] = ast.UnaryMinus(p[2])

    def p_postfix_expression_1(self, p):
        """ postfix_expression : primary_expression
        """
        p[0] = p[1]
    
    def p_postfix_expression_2(self, p):
        """ postfix_expression : postfix_expression LBRACKET expression RBRACKET
        """
        p[0] = ast.ListIndex(p[1], p[3])
    
    def p_postfix_expression_3(self, p):
        """ postfix_expression : postfix_expression LPAREN expression_list RPAREN
        """
        p[0] = ast.FunctionCall(p[1], p[3])
    
    def p_postfix_expression_4(self, p):
        """ postfix_expression : postfix_expression LPAREN RPAREN
        """
        p[0] = ast.FunctionCall(p[1])
    
    def p_postfix_expression_5(self, p):
        """ postfix_expression : postfix_expression DOT identifier_name
        """
        p[0] = ast.FieldReference(p[1], p[3])
    
    def p_primary_expression_1(self, p):
        """ primary_expression : identifier_name
                               | list_expression
                               | constant
                               | function_decl_expression
                               | lambda_expression
                               | dict_expression
        """
        p[0] = p[1]
        
    def p_primary_expression_2(self, p):
        """ primary_expression : LPAREN expression RPAREN
        """
        p[0] = p[2]
        
    def p_constant_1(self, p):
        """ constant : INT_LITERAL
                     | string_literal
        """
        p[0] = ast.Constant(p[1])
        
    def p_constant_2(self, p):
        """ constant : boolean_literal
        """
        p[0] = p[1]
        
    def p_string_literal(self, p):
        """ string_literal : QUOTATION_MARK string_chars QUOTATION_MARK
        """
        p[0] = p[2]
        
    def p_string_chars(self, p):
        """ string_chars :
                         | string_chars string_char
        """
        if len(p) == 1:
            p[0] = unicode()
        else:
            p[0] = p[1] + p[2]
        
    def p_string_char_1(self, p):
        """ string_char : UNESCAPED
        """
        p[0] = p[1]
        
    def p_string_char_2(self, p):
        """ string_char : ESCAPE QUOTATION_MARK
                        | ESCAPE BACKSLASH
                        | ESCAPE BACKSPACE_CHAR
                        | ESCAPE FORM_FEED_CHAR
                        | ESCAPE LINE_FEED_CHAR
                        | ESCAPE CARRIAGE_RETURN_CHAR
                        | ESCAPE TAB_CHAR
        """
        p[0] = p[2]
        
    def p_string_char_3(self, p):
        """ string_char : ESCAPE UNICODE_HEX
        """
        p[0] = unichr(int(p[2][1:], 16))
    
    def p_boolean_literal_1(self, p):
        """ boolean_literal : TRUE
        """
        p[0] = ast.Constant(True)
        
    def p_boolean_literal_2(self, p):
        """ boolean_literal : FALSE
        """
        p[0] = ast.Constant(False)
        
    def p_list_expression_1(self, p):
        """ list_expression : LBRACKET RBRACKET
        """
        p[0] = ast.List()
        
    def p_list_expression_2(self, p):
        """ list_expression : LBRACKET expression_list RBRACKET
        """
        p[0] = ast.List(p[2])
        
    def p_function_decl_expression_1(self, p):
        """ function_decl_expression : FUNC LPAREN identifier_list RPAREN compound_statement
        """
        p[0] = ast.FunctionDeclaration(p[3], p[5])
    
    def p_function_decl_expression_2(self, p):
        """ function_decl_expression : FUNC LPAREN RPAREN compound_statement
        """
        p[0] = ast.FunctionDeclaration([], p[4])
    
    def p_named_function_decl_1(self, p):
        """ named_function_decl : FUNC ID LPAREN identifier_list RPAREN compound_statement
        """
        p[0] = ast.NamedFunctionDeclaration(ast.Identifier(p[2]), p[4], p[6])
        
    def p_named_function_decl_2(self, p):
        """ named_function_decl : FUNC ID LPAREN RPAREN compound_statement
        """
        p[0] = ast.NamedFunctionDeclaration(ast.Identifier(p[2]), [], p[5])
    
    def p_lambda_expression_1(self, p):
        """ lambda_expression : LAMBDA identifier_list COLON expression
        """
        p[0] = ast.LambdaExpression(p[4], p[2])
    
    def p_lambda_expression_2(self, p):
        """ lambda_expression : LAMBDA COLON expression
        """
        p[0] = ast.LambdaExpression(p[3])
    
    def p_dict_expression_1(self, p):
        """ dict_expression : LBRACE RBRACE
        """
        p[0] = ast.Dict()
    
    def p_dict_expression_2(self, p):
        """ dict_expression : LBRACE key_value_list RBRACE
        """
        p[0] = ast.Dict(p[2])
        
    def p_key_value_list_1(self, p):
        """ key_value_list : expression COLON expression
        """
        p[0] = [ast.KeyValuePair(p[1], p[3])]
    
    def p_key_value_list_2(self, p):
        """ key_value_list : key_value_list COMMA expression COLON expression
        """
        p[0] = p[1] + [ast.KeyValuePair(p[3], p[5])]
    
    def p_expression_list(self, p):
        """ expression_list : expression
                            | expression_list COMMA expression
        """
        if len(p) == 2:
            p[0] = [p[1]] if p[1] else []
        else:
            p[0] = p[1] + ([p[3]] if p[3] else [])
            
    def p_identifier_list(self, p):
        """ identifier_list : ID
                            | identifier_list COMMA ID
        """
        if len(p) == 2:
            p[0] = [p[1]] if p[1] else []
        else:
            p[0] = p[1] + ([p[3]] if p[3] else [])
         
    def p_identifier_name(self, p):
        """ identifier_name : ID
        """
        p[0] = ast.Identifier(p[1])
        

class SWStatementParser(CloudScriptParser):
    
    def __init__(self):
        self.lexer = CloudScriptLexer()
        self.lexer.build()
        self.tokens = self.lexer.tokens
        self.parser = ply.yacc.yacc(module=self, start='statement')

class SWScriptParser(CloudScriptParser):
    
    def __init__(self):
        self.lexer = CloudScriptLexer()
        self.lexer.build()
        self.tokens = self.lexer.tokens
        self.parser = ply.yacc.yacc(module=self, start='script_file')

class SWExpressionParser(CloudScriptParser):
    
    def __init__(self):
        self.lexer = CloudScriptLexer()
        self.lexer.build()
        self.tokens = self.lexer.tokens
        self.parser = ply.yacc.yacc(module=self, start='expression')
        
if __name__ == '__main__':
    import sys
    csp = CloudScriptParser()
    result = csp.parse(open(sys.argv[1]).read())
    print result
