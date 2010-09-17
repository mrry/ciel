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
from skywriting.lang.visitors import Visitor
from skywriting.lang.parser import CloudScriptParser
import collections
import sys

class Open:
    def __init__(self, offset=0, consistent=False):
        self.offset = offset
        self.consistent = consistent

class Close:
    pass
CLOSE = Close()

class Break:
    def __init__(self, spaces=1, offset=0):
        self.blank_spaces = spaces
        self.offset = offset

BLANK = Break()
BLANK_NEWLINE = Break(spaces=100, offset=0)
BLANK_INDENT_NEWLINE = Break(spaces=100, offset=4)

class Eof:
    pass

EOF = Eof()

class PrettyPrinterVisitor(Visitor):
    
    def __init__(self, out_file=sys.stdout, line_width=80):
        self.out_file = sys.stdout
        self.line_width = line_width
        self.margin = line_width
        self.space = line_width
        
        self.n = 3 * self.margin
        
        self.top = 0
        self.bottom = 0
        
        self.left = 0
        self.right = 0
        
        self.tokens = []
        self.lengths = []
        self.scan_stack = collections.deque()
        self.print_stack = []
        self.left_total = 0
        self.right_total = 0
    
    def receive(self, symbol):
        
        #print >>sys.stderr, '***', self.left_total, self.right_total
        
        if symbol is EOF:
            if len(self.scan_stack) > 0:
                self.check_stack(0)
                self.advance_left(self.tokens[self.left], self.lengths[self.left])
            self.indent(0)
        
        elif isinstance(symbol, Open):
            print >>sys.stderr, '<Open:', self.left, self.right
            if len(self.scan_stack) == 0:
                self.left_total = 1
                self.right_total = 1
                self.left = 0
                self.right = 0
            else:
                self.right += 1
            self.tokens.append(symbol)
            self.lengths.append(-self.right_total)
            self.scan_stack.append(len(self.tokens) - 1)
            
        elif symbol is CLOSE:
            print >>sys.stderr, '<Close:', self.left, self.right
            if len(self.scan_stack) == 0:
                print >>sys.stderr, 'RC Left:', self.left, self.right
                self.print_symbol(symbol, 0)
            else:
                self.right += 1
                self.tokens.append(symbol)
                self.lengths.append(-1)
                self.scan_stack.append(len(self.tokens) - 1)
            
        elif isinstance(symbol, Break):
            if len(self.scan_stack) == 0:
                self.left_total = 1
                self.right_total = 1
                #self.left = 0
                #self.right = 0
            else:
                self.right += 1
            self.check_stack(0)

            self.tokens.append(symbol)
            self.lengths.append(-self.right_total)
            self.scan_stack.append(len(self.tokens) - 1)
            self.right_total += symbol.blank_spaces
            
        else:
            if len(self.scan_stack) == 0:
                print >>sys.stderr, 'RS Left:', self.left, self.right
                self.print_symbol(symbol, len(symbol))
            else:
                self.right += 1
                self.tokens.append(symbol)
                self.lengths.append(len(symbol))
                self.right_total += len(symbol)
                self.check_stream()
            
    def check_stream(self):        
        if self.right_total - self.left_total > self.space:
            if len(self.scan_stack) > 0:
                if self.left == self.scan_stack[0]:
                    self.lengths[self.scan_stack.popleft()] = 1000000
            self.advance_left(self.tokens[self.left], self.lengths[self.left])
            if self.left != self.right:
                self.check_stream()
    
    def advance_left(self, symbol, l):            
        if l >= 0 and self.left < self.right:
            print >>sys.stderr, 'AL Left:', self.left, self.right
            self.print_symbol(symbol, l)
            if isinstance(symbol, Break):
                self.left_total += symbol.blank_spaces
            elif isinstance(symbol, str):
                self.left_total += l
            if self.left < self.right:
                self.left += 1
                self.advance_left(self.tokens[self.left], self.lengths[self.left])
                
    def check_stack(self, k):
        if len(self.scan_stack) > 0:
            x = self.scan_stack[-1]
            symbol = self.tokens[x]
            if isinstance(symbol, Open):
                if k > 0:
                    self.lengths[self.scan_stack.pop()] += self.right_total
                    self.check_stack(k - 1)
            elif symbol is CLOSE:
                self.lengths[self.scan_stack.pop()] = 1
                self.check_stack(k + 1)
            else:
                #print '&&&', symbol
                self.lengths[self.scan_stack.pop()] += self.right_total
                if k > 0:
                    self.check_stack(k)
                
    def newline(self, amount):
        self.out_file.write('\n')
        self.indent(amount)
                
    def indent(self, amount):
        for _ in range(0, amount):
            self.out_file.write(' ')
            
    def print_symbol(self, symbol, l):
        if isinstance(symbol, Open):
            if l > self.space:
                self.print_stack.append((self.space - symbol.offset, symbol.consistent))    
            else:
                self.print_stack.append((0, None))
                
        elif symbol is CLOSE:
            self.print_stack.pop()
            
        elif isinstance(symbol, Break):
            offset, break_type = self.print_stack[-1]
            if break_type is None:
                # fits
                #print 'Fits', self.space, symbol.spaces
                self.space -= symbol.blank_spaces
                self.indent(symbol.blank_spaces)
            elif break_type:
                # consistent
                #print 'Consistent', self.space, symbol.spaces
                self.space = offset - symbol.offset
                self.newline(self.margin - self.space)
            else:
                # inconsistent
                #print 'Inconsistent', self.space, symbol.spaces
                if l > self.space:
                    self.space = offset - symbol.offset
                    self.newline(self.margin - self.space)
                else:
                    self.space -= symbol.blank_spaces
                    self.indent(symbol.blank_spaces)
        
        else:
            if l > self.space:
                print l, self.space
                
                print self.print_stack
                
                for tok, len in zip(self.tokens, self.lengths):
                    print repr(tok), len
                raise Exception('Line too long')
            self.space -= l
            self.out_file.write(symbol)
            #print >>sys.stderr, symbol
                 
    def open(self):
        return self.receive(Open())
    
    def close(self):
        return self.receive(CLOSE)
    
    def spc(self):
        return self.receive(Break(1, 2))
    
    def cr(self, indent=0):
        return self.receive(Break(100000, indent))
                    
    def visit(self, node, stack, stack_base):
        #print >>sys.stderr, 'Visiting %s' % node.__class__.__name__
        return getattr(self, "visit_%s" % (node.__class__.__name__, ))(node, stack, stack_base)
        
    def visit_statement_list(self, statements, stack, stack_base):
        for i, statement in enumerate(statements):
            self.visit(statement, stack, stack_base + 1)
            if i < len(statements) - 1:
                self.cr()
    
    def visit_Assignment(self, node, stack, stack_base):
        self.visit(node.lvalue, stack, stack_base + 1)
        self.spc()
        self.receive('=')
        self.spc()
        self.visit(node.rvalue, stack, stack_base + 1)
        self.receive(';')
    
    def visit_PlusAssignment(self, node, stack, stack_base):
        self.visit(node.lvalue, stack, stack_base + 1)
        self.spc()
        self.receive('+=')
        self.spc()
        self.visit(node.rvalue, stack, stack_base + 1)
        self.receive(';')
        
    def visit_Break(self, node, stack, stack_base):
        self.receive('break;')
    
    def visit_Continue(self, node, stack, stack_base):
        self.receive('continue;')
        
    def visit_Do(self, node, stack, stack_base):
        self.receive('do {')
        self.cr(4)
        self.visit_statement_list(node.body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('} while (')
        self.visit(node.condition, stack, stack_base + 1)
        self.receive(');')
    
    def visit_If(self, node, stack, stack_base):
        self.receive('if (')
        self.visit(node.condition, stack, stack_base + 1)
        self.receive(') {')
        self.cr(4)
        self.visit_statement_list(node.true_body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('}')
        if node.false_body is not None:
            self.receive(' else {')
            self.cr(4)
            self.visit_statement_list(node.false_body, stack, stack_base + 1)
            self.cr(-4)
            self.receive('}')
        
    def visit_For(self, node, stack, stack_base):
        self.receive('for (')
        self.visit(node.indexer, stack, stack_base + 1)
        self.receive(' in ')
        self.visit(node.iterator, stack, stack_base + 1)
        self.receive(') {')
        self.cr(4)
        self.visit_statement_list(node.body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('}')
        
    def visit_Return(self, node, stack, stack_base):
        self.receive('return ')
        self.visit(node.expr, stack, stack_base + 1)
        self.receive(';')
        
    def visit_While(self, node, stack, stack_base):
        self.receive('while (')
        self.visit(node.condition, stack, stack_base + 1)
        self.receive(') {')
        self.receive('{')
        self.cr(4)
        self.visit_statement_list(node.body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('}')
            
    def visit_Script(self, node, stack, stack_base):
        self.open()
        self.visit_statement_list(node.body, stack, stack_base)
        self.close()

    def visit_NamedFunctionDeclaration(self, node, stack, stack_base):
        self.receive('function')
        self.spc()
        self.visit(node.name, stack, stack_base + 1)
        self.spc()
        self.receive('(')
        for i, param in enumerate(node.formal_params):
            self.receive(param)
            if i < len(node.formal_params) - 1:
                self.receive(',')
                self.spc()
        self.receive(')')
        self.spc()
        self.receive('{')
        self.cr(4)
        self.visit_statement_list(node.body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('}')
        
    def visit_IdentifierLValue(self, node, stack, stack_base):
        self.receive(node.identifier)
        
    def visit_IndexedLValue(self, node, stack, stack_base):
        self.visit(node.base_lvalue, stack, stack_base)
        self.receive('[')
        self.visit(node.index, stack, stack_base)
        self.receive(']')
        
    def visit_FieldLValue(self, node, stack, stack_base):
        self.visit(node.base_lvalue, stack, stack_base)
        self.receive('[')
        self.visit(node.index, stack, stack_base)
        self.receive(']')
        
    def visit_And(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('&&')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
        
    def visit_Constant(self, node, stack, stack_base):
        self.receive(repr(node.value))
    
    def visit_Dereference(self, node, stack, stack_base):
        self.receive('*')
        self.visit(node.reference, stack, stack_base + 1)
    
    def visit_Dict(self, node, stack, stack_base):
        self.receive('{')
        for i, kvp in enumerate(node.items):
            self.visit(kvp, stack, stack_base + 1)
            if i < len(node.items) - 1:
                self.receive(',')
                self.spc()
        self.receive('}')
            
    def visit_Equal(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('==')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_FieldReference(self, node, stack, stack_base):
        self.visit(node.object, stack, stack_base)
        self.receive('.')
        self.receive(node.field)
    
    def visit_SpawnedFunction(self, node, stack, stack_base):
        self.visit(node.function, stack, stack_base)
        self.receive('(')
        for i, arg in enumerate(node.args):
            self.visit(arg, stack, stack_base)
            if i < len(node.args) - 1:
                self.receive(',')
                self.spc()
        self.receive(')')
        
    def visit_FunctionCall(self, node, stack, stack_base):
        self.visit(node.function, stack, stack_base + 1)
        self.receive('(')
        for i, arg in enumerate(node.args):
            self.visit(arg, stack, stack_base + 1)
            if i < len(node.args) - 1:
                self.receive(',')
                self.receive(BLANK)
        self.receive(')')

    def visit_FunctionDeclaration(self, node, stack, stack_base):
        self.receive('function (')
        for i, param in enumerate(node.formal_params):
            self.receive(param)
            if i < len(node.formal_params) - 1:
                self.receive(',')
                self.spc()
        self.receive(') {')
        self.cr(6)
        self.visit_statement_list(node.body, stack, stack_base + 1)
        self.cr(-4)
        self.receive('}')
    
    def visit_GreaterThan(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('>')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
        
    def visit_GreaterThanOrEqual(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('>=')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_Identifier(self, node, stack, stack_base):
        self.receive(str(node.identifier))

    def visit_KeyValuePair(self, node, stack, stack_base):
        self.visit(node.key_expr, stack, stack_base + 1)
        self.spc()
        self.receive(':')
        self.spc()
        self.visit(node.value_expr, stack, stack_base + 1)
    
    def visit_LambdaExpression(self, node, stack, stack_base):
        self.receive('lambda')
        self.spc()
        for i, param in enumerate(node.variables):
            self.receive(param)
            if i < len(node.variables) - 1:
                self.receive(',')
                self.spc()
        self.receive(':')
        self.spc()
        self.visit(node.expr, stack, stack_base + 1)
    
    def visit_LessThan(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('<')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_LessThanOrEqual(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('<=')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_List(self, node, stack, stack_base):
        self.receive('[')
        for i, elem in enumerate(node.contents):
            self.visit(elem, stack, stack_base + 1)
            if i < len(node.contents) - 1:
                self.receive(',')
                self.spc()
        self.receive(']')
        
    def visit_ListIndex(self, node, stack, stack_base):
        self.visit(node.list_expr, stack, stack_base + 1)
        self.receive('[')
        self.visit(node.index, stack, stack_base + 1)
        self.receive(']')
        
    def visit_Minus(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('-')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_Not(self, node, stack, stack_base):
        self.receive('!')
        self.visit(node.expr, stack, stack_base)
        
    def visit_UnaryMinus(self, node, stack, stack_base):
        self.receive('-')
        self.visit(node.expr, stack, stack_base)
        
    def visit_NotEqual(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('!=')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_Or(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('||')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)
    
    def visit_Plus(self, node, stack, stack_base):
        self.visit(node.lexpr, stack, stack_base + 1)
        self.spc()
        self.receive('+')
        self.spc()
        self.visit(node.rexpr, stack, stack_base + 1)

if __name__ == '__main__':
    csp = CloudScriptParser()
    result = csp.parse(open(sys.argv[1]).read())
    pp = PrettyPrinterVisitor()
    pp.visit(result, [], 0)
    pp.receive(EOF)

    print >>sys.stderr, pp.tokens

    #print
    #print
    #for tok, len in zip(pp.tokens, pp.lengths):
    #    print repr(tok), len
    