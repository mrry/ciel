#!/usr/bin/env python

import random
import json
import sys
clusters = []
for i in range(10):
   clusters.append( (random.uniform(0,100), random.uniform(0,100)) )

xmax=0
xmin=1000000
ymin=1000000
ymax=0
for (x,y) in clusters:
   size = random.gauss(100, 40)
   radius = random.gauss(10, 4)
   for i in range(int(size)):
      px = random.gauss(x, radius)
      py = random.gauss(y, radius)
      if xmax < px: xmax=px
      if xmin > px: xmin=px
      if ymax < py: ymax=py
      if ymin > py: ymin=py
      print json.dumps([ px, py])

print >> sys.stderr, json.dumps( [ [ xmin-1, ymin-1 ], [ xmax+1, ymax+1 ] ] )
