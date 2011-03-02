#!/bin/bash

SIZE=$1
MAXIT=$2
SCALE=$3

export CLASSPATH=$CLASSPATH:src/java/:../../src/java

for i in 0 1 2 3 4; do
	for j in 0 1 2 3 4; do
      echo "Generating tile ($i, $j)..."
		java skywriting.examples.mandelbrot.Mandelbrot $SIZE $SIZE 5 5 $i $j $MAXIT $SCALE
	done
done

TSIZE=`expr $SIZE / 5` 

echo "Merging tiles..."
java skywriting.examples.mandelbrot.Stitch 5 5 $TSIZE $TSIZE

