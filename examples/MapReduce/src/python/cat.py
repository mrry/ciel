#!/usr/bin/python
import fcpy
import shutil

fcpy.init()

for input in fcpy.inputs:
    # Copy the entire contents of input to output 0.
    shutil.copyfileobj(input, fcpy.outputs[0])


