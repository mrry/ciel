#!/bin/sh

ocamlopt -unsafe -nodynlink -inline 10000 -annot -w -8 binomial_parallel.ml -o binomial-ocaml
gcc -ffast-math -std=c99 -o binomial-c -Wall -lm -O3 binomial-parallel.c
export JAVA_OPTS=-server
scalac -optimise ScalaBinomialOptions.scala
javac *.java

(time ./remote-binomial.sh 0 `pwd` ./binomial-c) > results-c.txt 2>&1
(time ./remote-binomial.sh 0 `pwd` ./binomial-ocaml) > results-ocaml.txt 2>&1
(time ./remote-binomial.sh 0 `pwd` "java BinomialRun") > results-java.txt 2>&1
(time ./remote-binomial.sh 0 `pwd` "scala ScalaBinomialOptions") > results-scala.txt 2>&1
