#!/bin/sh 

s=100.0
k=100.0
t=1.0
v=0.3
rf=0.03
cp=-1
n=1000
chunk=10

# do serial version
serial=`./binomial-serial.py $s $k $t $v $rf $cp $n`
echo "S: $serial"

# do parallel version
IFS=""
cont=1
inf=""
while [ $cont -eq 1 ]; do
  x=`./binomial-parallel.py $s $k $t $v $rf $cp $n $chunk $inf`
  if [ $? -eq 1 ]; then
    cont=0
    parallel=$x
  fi
  inf=$x
done
echo "P: $parallel"
