
# This is a test of exec'ing with an input that isn't ready yet. The idea is it should stall when the executor tries to pull the reference.

import skypy
import cStringIO

def chargen(x):
    outstr = cStringIO.StringIO()
    for i in range(x):
        outstr.write("%d" % i)
    return outstr.getvalue()

def skypy_main():

    wc_input = skypy.spawn(lambda: chargen(1000000))
    wc_result = skypy.sync_exec("stdinout", {"inputs": [wc_input], "command_line":["wc", "-c"]}, 1)
    return skypy.deref_json(wc_result[0])
