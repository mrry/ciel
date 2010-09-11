#!/usr/bin/python

import subprocess

print "========================"
print "Phase 1: generating data"
print "========================"
for i in range(1, 10):
    this_chunks = i * 100
    gen_args = ["hadoop", "jar", "/usr/lib/hadoop/hadoop-0.20.1+169.68-examples.jar", "teragen", "-Dmapred.map.tasks=100", str(320000 * this_chunks), ("/user/tera-%d" % this_chunks)]
    print "Running", gen_args
    gen_proc = subprocess.Popen(gen_args)
    gen_proc.wait()

print "====================="
print "Phase 2: sorting data"
print "====================="

for i in range(1, 10):
    for j in range(1, 4):
        this_chunks = i * 100
        run_args = ["hadoop", "jar", "/usr/lib/hadoop/hadoop-0.20.1+169.68-examples.jar", "terasort", "-Dmapred.reduce.tasks=100", ("/user/tera-%d" % this_chunks), ("/user/tera-%d-%d-out" % (this_chunks, j))]
        print "Running", run_args
        run_proc = subprocess.Popen(run_args)
        run_proc.wait()
        copy_args = ["cp", "/var/log/hadoop-0.20/hadoop-hadoop-jobtracker-domU-12-31-39-0E-D8-13.log", ("/root/hadoop-%d-%d.log" % (this_chunks, j))]
        print "Running", copy_args
        copy_proc = subprocess.Popen(copy_args)
        copy_proc.wait()
