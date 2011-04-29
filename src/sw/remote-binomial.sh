#!/bin/bash -e

s=100.0
k=100.0
t=1.0
v=0.3
rf=0.03
cp=-1
n=800000
chunk=40000

total=$(( ($n / $chunk) + 1 ))
num=$1
shift
dir="$1"
shift
cmd="$*"
script="$(basename $0)"
fullcmd="$cmd $s $k $t $v $rf $cp $n $chunk"
HOSTS=( $(cat hosts.txt) )
if [ $num -eq $total ]; then
  $fullcmd 0
elif [ $num -eq 0 ]; then
  let num+=1
  $fullcmd 1 | ssh -c blowfish -A ${HOSTS[$num]} "cd $dir && ./$script $num $dir $cmd"
else
  let num+=1
  $fullcmd 0 | ssh -c blowfish -A ${HOSTS[$num]} "cd $dir && ./$script $num $dir $cmd"
fi
