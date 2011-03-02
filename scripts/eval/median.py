import numpy

import sys

nums = [float(x.strip()) for x in sys.stdin.readlines()]

print min(nums), numpy.median(nums), max(nums)
