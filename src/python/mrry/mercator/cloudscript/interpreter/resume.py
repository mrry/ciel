'''
Created on 30 Mar 2010

@author: dgm36
'''

class BinaryExpressionRR:
    
    def __init__(self):
        self.left = None

class FunctionCallRR:
    
    def __init__(self, num_args):
        self.args = [None for i in range(num_args)]
        
class ListRR:
    
    def __init__(self, length):
        self.items = [None for i in range(length)]
        
class DictRR:
    
    def __init__(self, num_items):
        self.contents = [None for i in range(num_items)]
        
class StatementListRR:
    
    def __init__(self):
        self.current_statement_index = 0
        
class DoRR:
    
    def __init__(self):
        self.done_body = False
        
class WhileRR:
    
    def __init__(self):
        self.done_condition = False
        
class IfRR:
    
    def __init__(self):
        self.condition = None
        
class ForRR:
    
    def __init__(self):
        self.iterator = None
        self.i = 0

class ListIndexRR:
    
    def __init__(self):
        self.list = None