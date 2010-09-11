#!/usr/bin/python

import sys
import subprocess

procs = []

for machine in sys.argv[1:]:
    procs.append(subprocess.Popen(["scp", "-i", "/local/scratch/cs448/mercator/mercator.hg/scripts/skywriting-keypair.pem", "local_gensort.py", "root@" + machine + ":"]))

for proc in procs:
    proc.wait()
