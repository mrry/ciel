#!/usr/bin/python
import numpy as np
import sys,math,json
import getopt

def init_vals(t,v,rf,n):
  h = t/n
  u = math.exp( (rf - 0.5 * math.pow(v,2)) * h + v * math.sqrt(h) )
  d = math.exp( (rf - 0.5 * math.pow(v,2)) * h - v * math.sqrt(h) )
  drift = math.exp(rf*h)
  q = (drift-d)/(u-d)
  return (q,u,d,drift)

def calc_stkval(n, s, u, d):
  stkval = np.zeros((n+1,n+1))
  stkval[0,0]=s
  for i in range(1,n+1):
    stkval[i,0] = stkval[i-1,0]*u
    for j in range(1,i+1):
      stkval[i,j]=stkval[i-1,j-1]*d
  return stkval

# generate the bottom options row from stock value
def bottom_opt_row(stkval, n, k, cp):
  res=[]
  for j in range(n+1):
    v = max( 0.0, cp * (stkval[n,j] - k))
    res.append(v)
  return res

def do_one_row(rowin, rownum, q, drift):
   res=[]
   i=rownum-1
   for j in range(rownum):
     v = (q * rowin[j] + (1-q) * rowin[j+1]) / drift
     res.append(v)
   return res

def main():
  # usage: script s k t v rf cp n chunksize '{"row":[],"pos":pos}'
  s=float(sys.argv[1])
  k=float(sys.argv[2])
  t=float(sys.argv[3])
  v=float(sys.argv[4])
  rf=float(sys.argv[5])
  cp=float(sys.argv[6])
  n=int(sys.argv[7])
  chunksize=int(sys.argv[8])
  (q,u,d,drift)=init_vals(t,v,rf,n)
  if len(sys.argv) < 10:
    # generate bottom optval from stkval 
    stkval=calc_stkval(n,s,u,d)
    optval_bottom=bottom_opt_row(stkval,n,k,cp)
    print json.dumps({'row':optval_bottom,'pos':n})
  else: 
    input_json=json.loads(sys.argv[9])
    lastrow=input_json['row']
    rowfrom=input_json['pos']
    rowto=max(1,rowfrom-chunksize)
    for i in range(rowfrom,rowto-1,-1):
      lastrow=do_one_row(lastrow, i, q, drift)
    if len(lastrow)==1:
      print str(lastrow[0])
      exit(1)
    else:
      print json.dumps({'row':lastrow,'pos':rowto-1})

if __name__=="__main__":
  main ()
