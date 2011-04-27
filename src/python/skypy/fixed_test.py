
from __future__ import with_statement

import skypy

def skypy_main():
    
    ret = skypy.spawn(lambda i: "Child returns %d" % i, 1)
    skypy.deref(ret)
    ret = skypy.spawn(lambda i: "Child returns %d" % i, 2)
    skypy.deref(ret)
    
    return 5