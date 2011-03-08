
from __future__ import with_statement

import sys
import skypy

def skypy_main():

    print >>sys.stderr, "SkyPy example producer:", len(skypy.extra_outputs), "outputs"

    for i, id in enumerate(skypy.extra_outputs):
        with skypy.open_output(id) as file_out:
            file_out.fp.write("Skypy writing output %d" % i)

    return "Wrote %d outputs" % len(skypy.extra_outputs)

