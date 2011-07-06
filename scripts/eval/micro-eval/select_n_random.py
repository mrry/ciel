#!/usr/bin/python

import sys
import random

with open(sys.argv[1]) as f:
    f.readline()
    for line in  random.sample(f.readlines(), int(sys.argv[2])):
        print line.strip()

