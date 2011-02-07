#!/usr/bin/python

import random
import sys

n_nodes = int(sys.argv[1])

for i in range(n_nodes):
    n_connections = int(random.expovariate(0.1))
    for j in range(n_connections):
        print i, int(random.uniform(0, (n_nodes - 1)))

