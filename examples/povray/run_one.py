#!/usr/bin/python

import subprocess,sys,os

pov_scene = "/usr/share/doc/povray/examples/radiosity/cornell.pov"
pov_bin = "povray"

def run_povray(tw,th,x1,y1,x2,y2,fname):
   cmd = [pov_bin, pov_scene, "+W%d" % tw, "+H%d" % th, "+SR%d"%x1, "+ER%d"%x2, "+SC%d"%y1, "+EC%d"%y2, "+FP", "+O"+fname]
   print cmd
   subprocess.call(cmd)

def main():
   outname = os.getenv("OUTPUT_FILES")
   outf = open(outname, 'r')
   fname = outf.readline().strip()
   gridx = int(sys.argv[1])
   gridy = int(sys.argv[2])
   x = int(sys.argv[3])
   y = int(sys.argv[4])
   sz = int(sys.argv[5])
   run_povray(gridx, gridy, x,y,x+sz,y+sz,fname)
   

if __name__ == '__main__':
    main()
