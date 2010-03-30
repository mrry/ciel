def evaluate(current_context):
    visitor = InterpreterVisitor(current_context)
    visitor.resume()
    return current_context

class ExecutionContext:

    def get_return_value(self):
        pass
    
    
    
    pass

class Visitor:
    
    def visit(self, node):
        return getattr(self, "visit_%s" % (str(node.__class__).split('.')[-1], ))(node)

class InterpreterVisitor(Visitor):

    def __init__(self, context):
        pass

    