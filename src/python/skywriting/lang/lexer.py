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
from skywriting.runtime.exceptions import RuntimeSkywritingError,\
    SkywritingParsingError

'''
Created on 22 Feb 2010

@author: dgm36
'''
import ply.lex
from ply.lex import TOKEN

class CloudScriptLexer:
    
    def __init__(self):
        self.text = None
        pass
        
    def build(self, **kwargs):
        self.lexer = ply.lex.lex(object=self, **kwargs)
        
    def input(self, text):
        self.text = text
        self.lexer.input(text)
        
    def token(self):
        tok = self.lexer.token()
        return tok

    states = (('string', 'exclusive'), ('escaped', 'exclusive'))

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
                 'lambda' : 'LAMBDA',
                 'include' : 'INCLUDE' }
    
    tokens = keywords.values() + \
        ['ID', 'INT_LITERAL', 
         'PLUS', 'MINUS',
         'OR', 'AND', 'NOT', 'LT', 'LEQ', 'GT', 'GEQ', 'EQ', 'NE',
         'ASSIGN', 'ASSIGN_PLUS',
         'STAR', 'DOT',
         'LPAREN', 'RPAREN',
         'LBRACKET', 'RBRACKET',
         'LBRACE', 'RBRACE',
         'SEMICOLON', 'COMMA', 'COLON',
         'COMMENT_SINGLELINE', 'COMMENT_MULTILINE',
         'QUOTATION_MARK',
         'UNESCAPED', 'ESCAPE', 'BACKSLASH', 'BACKSPACE_CHAR', 'FORM_FEED_CHAR',
         'LINE_FEED_CHAR', 'CARRIAGE_RETURN_CHAR', 'TAB_CHAR', 'UNICODE_HEX']
    
    identifier = r'[a-zA-Z_][a-zA-Z_0-9]*'
    integer_literal = r'(0)|([1-9][0-9]*)'
    
    def t_error(self, t):
        raise SkywritingParsingError('Illegal character "%s" on line %d at column %d' % (t.value[0], self.lexer.lineno, self.find_column(t)))
        
    def t_escaped_error(self, t):
        raise SkywritingParsingError('Illegal escape character "%s" on line %d at column %d' % (t.value[0], self.lexer.lineno, self.find_column(t)))
        
    def t_string_error(self, t):
        raise SkywritingParsingError('Illegal character in string "%s" on line %d at column %d' % (t.value[0], self.lexer.lineno, self.find_column(t)))

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

    # Comment handling from CppHeaderParser
    def t_COMMENT_SINGLELINE(self, t):
        r'\/\/.*\n'
        return None
    
    def t_COMMENT_MULTILINE(self, t):
        r'/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/'
        return None

    # String handling from jsonply.

    def t_QUOTATION_MARK(self, t):
        r'\x22'
        t.lexer.push_state('string')
        return t

    t_string_ignore = ''
    t_string_UNESCAPED = r'[\x20-\x21,\x23-\x5B,\x5D-\xFF]+'
    
    def t_string_QUOTATION_MARK(self, t):
        r'\x22'
        t.lexer.pop_state()
        return t
        
    def t_string_ESCAPE(self, t):
        r'\x5C'
        t.lexer.push_state('escaped')
        return t
    
    t_escaped_ignore = ''
    
    def t_escaped_QUOTATION_MARK(self, t):
        r'\x22'
        t.lexer.pop_state()
        return t
    
    def t_escaped_BACKSLASH(self, t):
        r'\x5C'
        t.lexer.pop_state()
        return t
    
    def t_escaped_BACKSPACE_CHAR(self, t):
        r'\x62'
        t.lexer.pop_state()
        t.value = chr(0x0008)
        return t
        
    def t_escaped_FORM_FEED_CHAR(self, t):
        r'\x66'
        t.lexer.pop_state()
        t.value = chr(0x000c)
        return t
    
    def t_escaped_CARRIAGE_RETURN_CHAR(self, t):
        r'\x72'
        t.lexer.pop_state()
        t.value = chr(0x000d)
        return t
        
    def t_escaped_LINE_FEED_CHAR(self, t):
        r'\x6E'
        t.lexer.pop_state()
        t.value = chr(0x000a)
        return t

    def t_escaped_TAB_CHAR(self, t):
        r'\x74'
        t.lexer.pop_state()
        t.value = chr(0x0009)
        return t
    
    def t_escaped_UNICODE_HEX(self, t):
        r'\x75[\x30-\x39,\x41-\x46,\x61-\x66]{4}'
        t.lexer.pop_state()
        return t
    
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

    def find_column(self, token):
        last_cr = self.text.rfind('\n',0,token.lexpos)
        if last_cr < 0:
            last_cr = 0
        column = (token.lexpos - last_cr) + 1
        return column
    
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
    
    
    
