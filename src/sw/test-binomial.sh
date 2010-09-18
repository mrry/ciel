#!/bin/bash -e

s=100.0
k=100.0
t=1.0
v=0.3
rf=0.03
cp=-1
n=100
chunk=10

ocamlopt -g -annot -w -8 bigarray.cmxa unix.cmxa binomial_parallel.ml  -o binomial-parallel

# do serial version
echo -n 'S: '
./binomial-serial.py $s $k $t $v $rf $cp $n

# do parallel version
IFS=""
cmd="./binomial-parallel $s $k $t $v $rf $cp $n $chunk 1"
x=$(($n+$chunk))
while [ $x -gt 0 ]; do
  cmd="$cmd | ./binomial-parallel $s $k $t $v $rf $cp $n $chunk 0"
  let x-=$chunk || true
done
echo -n 'P: '
bash -c "$cmd"
