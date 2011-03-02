#!/bin/bash

cat $INPUT_FILES
echo
cat $OUTPUT_FILES

datafile=`cat $INPUT_FILES | head -n 1`
wfile=`cat $INPUT_FILES | tail -n +2 | head -n 1`
outfile=`cat $OUTPUT_FILES | head -n 1`

echo "$LR_MAP_EXEC $DIM $datafile $wfile $outfile $PARSE"
$LR_MAP_EXEC $DIM $datafile $wfile $outfile $PARSE