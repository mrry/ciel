
from __future__ import with_statement

import skypy
import sys

# A silly script to check that deref'ing something that isn't available within a requirement block behaves correctly.

def spawnee(i):
    sys.stderr.write("Hello from spawnee %d" % i)
    return i*i

def skypy_main():

    spawned = [skypy.spawn(lambda: spawnee(i)) for i in range(10)]
    spawn_rets = dict()
    with skypy.RequiredRefs(spawned[1:6]):
        for (i, x) in enumerate(spawned):
            spawn_rets[i] = skypy.deref(x)
        for (key, value) in spawn_rets.iteritems():
            sys.stderr.write("Spawned task %d returned %d\n" % (key, value))

