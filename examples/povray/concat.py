#!/usr/bin/python

import sys,os

def main():
  gridx = int(sys.argv[1])
  gridy = int(sys.argv[2])
  ifiles = os.getenv("INPUT_FILES")
  ofiles = os.getenv("OUTPUT_FILES")
  outf = open(ofiles, 'r')
  ofname = outf.readline().strip()
  outf.close()
  fout = open(ofname,'wb')
  header = "P6\n%d %d\n255\n" % (gridx,gridy)
  hlen = len(header)
  fout.write(header)
  sz = int(sys.argv[3])
  rowlen = 3*gridx
  inf = open(ifiles, 'r')
  ifnames = map(lambda x: x.strip(), inf.readlines())
  inf.close()
  for y in range(0,gridy):
    for x in range(0,gridx):
      fout.write('AAA')
  for i in range(len(ifnames)):
    x1 = i % (gridx / sz)
    y1 = i / (gridx / sz)
    x2 = x1 + sz
    y2 = y1 + sz
    rin = open(ifnames[i], 'rb')
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
  fout.close()

if __name__=="__main__":
  main ()
