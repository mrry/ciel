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
Created on 30 Mar 2010

@author: dgm36
'''

class BinaryExpressionRR:
    
    def __init__(self):
        self.left = None
        
    def __repr__(self):
        return '%s(left=%s)' % (self.__class__.__name__, repr(self.left), )

class PlusRR(BinaryExpressionRR):
    pass
class LessThanOrEqualRR(BinaryExpressionRR):
    pass
class EqualRR(BinaryExpressionRR):
    pass
class StarRR(BinaryExpressionRR):
    pass

class ForceEvalRR:
    
    def __init__(self):
        self.maybe_wrapped = None
        
    def __repr__(self):
        return 'ForceEvalRR(maybe_wrapped=%s)' % (repr(self.maybe_wrapped), )

class FunctionCallRR:
    
    def __init__(self, num_args):
        self.args = [None for _ in range(num_args)]
        
    def __repr__(self):
        return 'FunctionCallRR(args=%s)' % (repr(self.args), )
        
class ListRR:
    
    def __init__(self, length):
        self.items = [None for _ in range(length)]
        
    def __repr__(self):
        return 'ListRR(items=[%s])' % (repr(self.items), )
        
class DictRR:
    
    def __init__(self, num_items):
        self.contents = [None for _ in range(num_items)]
        
    def __repr__(self):
        return 'DictRR(contents={%s})' % (repr(self.contents), )
        
class StatementListRR:
    
    def __init__(self):
        self.current_statement_index = 0
        
    def __repr__(self):
        return 'StatementListRR(current_statement_index=%d)' % (self.current_statement_index, )
        
class DoRR:
    
    def __init__(self):
        self.done_body = False
    
    def __repr__(self):
        return 'DoRR(done_body=%s)' % (repr(self.done_body), )
        
class WhileRR:
    
    def __init__(self):
        self.done_condition = False
        
    def __repr__(self):
        return 'WhileRR(done_condition=%s)' % (repr(self.done_condition), )
        
class IfRR:
    
    def __init__(self):
        self.condition = None
    
    def __repr__(self):
        return 'IfRR(condition=%s)' % (repr(self.condition), )
        
class ForRR:
    
    def __init__(self):
        self.iterator = None
        self.i = 0

    def __repr__(self):
        return 'ForRR(iterator=%s, i=%s)' % (repr(self.iterator), repr(self.i))
    
class ListIndexRR:
    
    def __init__(self):
        self.list = None
    
    def __repr__(self):
        return 'ListIndexRR(list=%s)' % (repr(self.list), )
       
class AssignmentRR:
    def __init__(self):
        self.rvalue = None
    def __repr__(self):
        return 'AssignmentRR(rvalue=%s)' % (repr(self.rvalue), )

class PlusAssignmentRR:
    def __init__(self):
        self.rvalue = None
    def __repr__(self):
        return 'PlusAssignmentRR(rvalue=%s)' % (repr(self.rvalue), )
        
class ContextAssignRR:
    
    def __init__(self):
        self.base_lvalue = None
    
    def __repr__(self):
        return 'ContextAssignRR(base_lvalue=%s)' % (repr(self.base_lvalue), )
        
class IndexedLValueRR:
    
    def __init__(self):
        self.index = None
    
    def __repr__(self):
        return 'IndexedLValueRR(index=%s)' % (repr(self.index), )
    
class ReturnRR:

    def __init__(self):
        self.ret = None
        
    def __repr__(self):
        return 'ReturnRR(ret=%s)' % (repr(self.ret), )