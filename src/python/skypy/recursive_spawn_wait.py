
from __future__ import with_statement

import skypy
import sys

def spawnee(i):
    sys.stderr.write("Hello from spawnee at layer %d\n" % i)
    if i == 0:
        return 1
    else:
        spawnees = [skypy.spawn(lambda: spawnee(i - 1)) for j in range(2)]
        with skypy.RequiredRefs(spawnees):
            results = [skypy.deref(x) for x in spawnees]
        accum = 0
        for result in results:
            accum += result
        return accum

def skypy_main():

    sys.stderr.write("Main start\n");
    ref = skypy.spawn(lambda: spawnee(8))
    answer = skypy.deref(ref)
    sys.stderr.write("Final result: %d\n" % answer)
    return answer

