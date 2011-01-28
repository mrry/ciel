
import skypy
import sys

def skypy_main():

    wc_source = skypy.spawn_exec("grab", {"urls":["http://www.gutenberg.org/cache/epub/4908/pg4908.html"], "version":0}, 1)
    wc_input = skypy.deref_json(wc_source[0]) # Yields a single reference
    wc_result = skypy.spawn_exec("stdinout", {"inputs": [wc_input], "command_line":["wc", "-c"]}, 1)
    return skypy.deref_json(wc_result[0])
