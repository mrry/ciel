#!/usr/bin/python

import subprocess

pov_scene = "cornell.pov"
pov_bin = "povray"

tw = 300
th = 300
sz = 50

def run_povray(x1,y1,x2,y2):
   fname = "out-%d-%d-%d-%d.ppm" % (y1,x1,y2,x2)
   cmd = [pov_bin, pov_scene, "+W%d" % tw, "+H%d" % th, "+SR%d"%x1, "+ER%d"%x2, "+SC%d"%y1, "+EC%d"%y2, "+FP", "+O"+fname]
   print cmd
   subprocess.call(cmd)

def main():
   
   cw = 0
   ch = 0
   for w in range(0,tw,sz):
     for h in range(0,th,sz):
       x1 = w
       y1 = h
       x2 = w+sz
       y2 = h+sz
       run_povray(x1,y1,x2,y2)
   

if __name__ == '__main__':
    main()
