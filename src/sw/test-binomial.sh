#!/bin/bash -e

s=100.0
k=100.0
t=1.0
v=0.3
rf=0.03
cp=-1
n=50000
chunk=10000

ocamlopt -unsafe -nodynlink -inline 10000 -annot -w -8 binomial_parallel.ml -o binomial-ocaml
gcc -ffast-math -std=c99 -o binomial-c -Wall -lm -O3 binomial-parallel.c
scalac ScalaBinomialOptions.scala
javac *.java

# do serial version
#echo "Running: ./binomial-serial.py"
#time ./binomial-serial.py $s $k $t $v $rf $cp $n

function run_parallel {
  BINP=$1
  IFS=""
  cmd="$BINP $s $k $t $v $rf $cp $n $chunk 1"
  x=$(($n+$chunk))
  while [ $x -gt 0 ]; do
    cmd="$cmd | $BINP $s $k $t $v $rf $cp $n $chunk 0"
    let x-=$chunk || true
  done
  echo "Running: $BINP"
  time bash -c "$cmd"
}

run_parallel "java BinomialRun"
run_parallel "scala ScalaBinomialOptions"
run_parallel "./binomial-c"
run_parallel "./binomial-ocaml"
#run_parallel "./binomial-python"
