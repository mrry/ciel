#!/bin/bash

s=100.0
k=100.0
t=1.0
v=0.3
rf=0.03
cp=-1
n=10000
chunk=1000

ocamlopt -g -annot -w -8 bigarray.cmxa binomial_parallel.ml  -o binomial-parallel

# do serial version
serial=`time ./binomial-serial.py $s $k $t $v $rf $cp $n`
echo "S: $serial"

# do parallel version
IFS=""
cmd="./binomial-parallel $s $k $t $v $rf $cp $n $chunk 1"
x=$(($n+$chunk))
while [ $x -gt 0 ]; do
  cmd="$cmd | ./binomial-parallel $s $k $t $v $rf $cp $n $chunk 0"
  let x-=$chunk
done
time bash -c "$cmd"
