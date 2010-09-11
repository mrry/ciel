#!/usr/bin/python

import sys
import subprocess

procs = []

start_rec = 0
recs_per_machine = 100000000

for machine in sys.argv[1:]:
    out_file = open("/tmp/posted_stuff/" + machine, "w")
    procs.append(subprocess.Popen(["ssh", "-i", "/local/scratch/cs448/mercator/mercator.hg/scripts/skywriting-keypair.pem", "root@" + machine, "/root/local_gensort.py", str(start_rec), str(recs_per_machine)], stdout = out_file))
    out_file.close()
    start_rec = start_rec + recs_per_machine

for proc in procs:
    proc.wait()
