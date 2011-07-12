#!/usr/bin/python

import sys
import random

with open(sys.argv[1]) as f:
    f.readline()
    prod, cons = random.sample(f.readlines(), 2)

    print "export HOST_FROM=%s; export HOST_TO=%s" % (prod.strip(), cons.strip())
