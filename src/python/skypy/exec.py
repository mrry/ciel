
import skypy
import sys

def skypy_main():

    wc_source = skypy.sync_exec("grab", {"urls":["http://www.srcf.ucam.org/~cs448/Scavengers_0002.wmv"], "version":0}, 1)
    wc_input = skypy.deref_json(wc_source[0]) # Yields a single reference
    wc_result = skypy.sync_exec("stdinout", {"inputs": [wc_input], "command_line":["wc", "-c"]}, 1)
    return skypy.deref_json(wc_result[0])
