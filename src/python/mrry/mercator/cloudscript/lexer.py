'''
Created on 22 Feb 2010

@author: dgm36
'''
import ply.lex
from ply.lex import TOKEN

class CloudScriptLexer:
    
    def __init__(self):
        pass
    
    def build(self, **kwargs):
        self.lexer = ply.lex.lex(object=self, **kwargs)
        
    def input(self, text):
        self.lexer.input(text)
        
    def token(self):
        tok = self.lexer.token()
        return tok
    
    keywords = { 'function' : 'FUNC',
                 'if' : 'IF',
                 'else' : 'ELSE',
                 'for' : 'FOR',
                 'in' : 'IN',
                 'while' : 'WHILE',
                 'do' : 'DO',
                 'return' : 'RETURN',
                 'break' : 'BREAK',
                 'continue' : 'CONTINUE',
                 'true' : 'TRUE',
                 'false' : 'FALSE',
                 'lambda' : 'LAMBDA' }
    
    tokens = keywords.values() + \
        ['ID', 'INT_LITERAL', 'STRING_LITERAL', 
         'PLUS', 'MINUS',
         'OR', 'AND', 'NOT', 'LT', 'LEQ', 'GT', 'GEQ', 'EQ', 'NE',
         'ASSIGN', 'ASSIGN_PLUS',
         'STAR', 'DOT',
         'LPAREN', 'RPAREN',
         'LBRACKET', 'RBRACKET',
         'LBRACE', 'RBRACE',
         'SEMICOLON', 'COMMA', 'COLON']
    
    identifier = r'[a-zA-Z_][a-zA-Z_0-9]*'
    integer_literal = r'(0)|([1-9][0-9]*)'
    string_literal = r'"[^"\\\n]*"'
    
    
    @TOKEN(identifier)
    def t_ID(self, t):
        t.type = self.keywords.get(t.value, 'ID')
        return t
    
    @TOKEN(integer_literal)
    def t_INT_LITERAL(self, t):
        t.value = int(t.value)
        return t
    
    t_PLUS = r'\+'
    t_MINUS = r'\-'
    t_OR = r'\|\|'
    t_AND = r'&&'
    t_NOT = r'!'
    t_LT = r'<'
    t_GT = r'>'
    t_LEQ = r'<='
    t_GEQ = r'>='
    t_EQ = r'=='
    t_NE = r'!='
    t_SEMICOLON = r';'
    t_STRING_LITERAL = string_literal
    
    t_ASSIGN = r'='
    t_ASSIGN_PLUS = r'\+='
    
    t_STAR = r'\*'
    t_DOT = r'\.'
    
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_LBRACE = r'\{'
    t_RBRACE = r'\}'
    t_LBRACKET = r'\['
    t_RBRACKET = r'\]'
    t_COMMA = r','
    t_COLON = r':'
    
    t_ignore = ' \t'
    
    def t_NEWLINE(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
if __name__ == '__main__':
    text = open('testscript.sw').read()
    
    csl = CloudScriptLexer()
    csl.build()
    csl.input(text)
    
    while 1:
        tok = csl.token()
        if not tok:
            break
        
        print "-", tok.value, tok.type, tok.lineno
    
    
    
