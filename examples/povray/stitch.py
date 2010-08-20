#!/usr/bin/python
import sys,os,re

tw=300
th=300

def main():
   fname = sys.argv[1]
   fin = open(fname, 'r')
   lines = fin.readlines()
   lines = map(lambda x: os.path.basename(x.strip()), lines)
   fin.close()
   coords = map(lambda x: map(int,re.split('-|\.',x)[1:5]), lines)
   # print gray image
   fout = open("stitch.ppm",'wb')
   header = "P6\n%d %d\n255\n" % (tw,th)
   hlen = len(header)
   rowlen = 3 * tw
   print hlen
   fout.write(header)
   for y in range(0,th):
     for x in range(0,tw):
       fout.write('AAA')
   # default gray written, now go through grid
   print coords
   for c in coords:
     x1=c[0]
     y1=c[1]
     x2=c[2]
     y2=c[3]
     rin = open("out-%d-%d-%d-%d.ppm" % (x1,y1,x2,y2),'rb')
     # skip ppm header
     f=rin.readline()
     f+=rin.readline()
     f+=rin.readline()
     print len(f)
     rin.seek(len(f))
     # iterate through the files rows
     for y in range(y1,y2):
         # povray outputs a whole row even for partial
         row = rin.read(rowlen)
         # take the slice we want
         row = row[(x1*3):(x2*3)]
         fout.seek(hlen+(y*rowlen)+(x1*3))
         fout.write(row)
     foo=len(rin.read(1000))
     print foo
     #assert(foo == 0)
     rin.close()
   fout.close()

if __name__ == '__main__':
   main ()
