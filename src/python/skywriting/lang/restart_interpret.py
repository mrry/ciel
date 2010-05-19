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
Created on 31 Mar 2010

@author: dgm36
'''
from skywriting.lang.visitors import StatementExecutorVisitor,\
    ExecutionInterruption
from skywriting.lang.parser import CloudScriptParser
from skywriting.lang.context import SimpleContext
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
    
    