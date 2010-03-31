'''
Created on 31 Mar 2010

@author: dgm36
'''
from mrry.mercator.cloudscript.visitors import StatementExecutorVisitor,\
    ExecutionInterruption
from mrry.mercator.cloudscript.parser import CloudScriptParser
from mrry.mercator.cloudscript.context import SimpleContext
import sys
import cPickle

if __name__ == '__main__':
    

    if sys.argv[1] == "--start":
        csp = CloudScriptParser()
        with open(sys.argv[2], "r") as scriptfile:
            script = csp.parse(scriptfile.read())
        ctxt = SimpleContext()
        stack = []
    else:
        (script, ctxt, stack) = cPickle.load(open(sys.argv[2]))
    
    print script
    
    try:
        ctxt.restart()
        StatementExecutorVisitor(ctxt).visit(script, stack, 0)
        print "Completed execution!"
        print ctxt.contexts
    except ExecutionInterruption:
        print "Dumping continuation to %s" % (sys.argv[3], )
        with open(sys.argv[3], "w") as continuation_file:
            cPickle.dump((script, ctxt, stack), continuation_file)
    
    