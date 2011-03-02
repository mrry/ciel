#!/bin/sh

DIM=10
NPOINTS=100000
SEED=42
SEP=4

./lr-datagen $DIM $NPOINTS $SEED $SEP | ./lr-parsedata 10 > data.parsed
./lr-wgen $DIM $SEED > wfile

./lr-map $DIM data.parsed wfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0
./lr-map $DIM data.parsed outfile outfile 0