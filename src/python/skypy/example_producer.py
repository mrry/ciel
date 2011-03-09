
from __future__ import with_statement

import sys
import skypy

def skypy_main():

    print >>sys.stderr, "SkyPy example producer:", len(skypy.extra_outputs), "outputs"

    for i, id in enumerate(skypy.extra_outputs):
        with skypy.open_output(id) as file_out:
            file_out.write("Skypy writing output %d" % i)

    for i in range(3):
        name = skypy.get_fresh_output_name()
        with skypy.open_output(name) as file_out:
            file_out.write("Skypy writing anonymous output %d" % i)

    return "Wrote %d external outputs and 3 I created myself" % len(skypy.extra_outputs)

