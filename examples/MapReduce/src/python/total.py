#!/usr/bin/python
import fcpy
import shutil

fcpy.init()

count = 0

for input in fcpy.inputs:
    # Copy the entire contents of input to output 0.
    for line in input.readlines():
        count += int(line)

print >>fcpy.outputs[0], count



