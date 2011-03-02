#!/bin/sh

DIM=10
NPOINTS=100000
SEED=42
SEP=4

./lr-datagen $DIM $NPOINTS $SEED $SEP > data.txt
./lr-wgen $DIM $SEED > wfile

./lr-map $DIM data.txt wfile outfile 1
./lr-map $DIM data.txt outfile outfile 1
./lr-map $DIM data.txt outfile outfile 1
./lr-map $DIM data.txt outfile outfile 1
./lr-map $DIM data.txt outfile outfile 1
./lr-map $DIM data.txt outfile outfile 1
./lr-map $DIM data.txt outfile outfile 1