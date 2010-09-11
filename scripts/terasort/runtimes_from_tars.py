#!/usr/bin/python

import sys
import tempfile
import subprocess
import shutil

devnull = open("/dev/null", "w")

for tarfile in sys.argv[1:]:
    tempdir = tempfile.mkdtemp()
    untar_proc = subprocess.Popen(["tar", "xvjf", tarfile], cwd = tempdir, stdout = devnull)
    untar_proc.wait()
    outfile = open("%s/tmp/master_out" % tempdir, "r")
    decode_proc = subprocess.Popen(["get_sw_runtime.py"], stdin = outfile, stdout = subprocess.PIPE)
    runtime = decode_proc.stdout.read()
    sys.stdout.write("%s %s" % (tarfile, runtime))
    shutil.rmtree(tempdir)
    
