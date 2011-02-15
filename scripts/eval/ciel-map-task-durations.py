#!/usr/bin/python

import math
import sys
import matplotlib.pyplot as plt

sys.stdin.readline()

for line in sys.stdin.readlines():
    fields = line.split()
    
    start = float(fields[4])
    end = float(fields[5])
    worker = fields[11]

    if fields[8] == '14':
        print fields[6]


