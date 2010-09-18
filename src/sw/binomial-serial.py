#!/usr/bin/python

import numpy as np
import math,sys

def jarrow_rudd(s,k,t,v,rf,cp,am=False,n=100):
  h = t/n
  u = math.exp((rf-0.5*math.pow(v,2))*h+v*math.sqrt(h))
  d = math.exp((rf-0.5*math.pow(v,2))*h-v*math.sqrt(h))
  drift = math.exp(rf*h)
  q = (drift-d)/(u-d)
  stkval = np.zeros((n+1,n+1))
  optval = np.zeros((n+1,n+1))
  stkval[0,0]=s
  for i in range(1,n+1):
    stkval[i,0] = stkval[i-1,0]*u
    for j in range(1,i+1):
      stkval[i,j]=stkval[i-1,j-1]*d
  for j in range(n+1): 
    v=max(0,cp*(stkval[n,j]-k))
    optval[n,j]=v
  for i in range(n-1, -1, -1):
    for j in range(i+1):
      optval[i,j] = (q*optval[i+1,j]+(1-q) * optval[i+1,j+1])/drift
  return optval[0,0]

def main():
  print jarrow_rudd(float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), int(sys.argv[6]), False, int(sys.argv[7]))

if __name__=="__main__":
  main()
#print jarrow_rudd(100.0,100.0, 1.0, 0.3, 0.03, -1, False, 100)
