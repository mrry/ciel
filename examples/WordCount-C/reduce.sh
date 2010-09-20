#!/bin/bash
OUTPUT_FILENAME=`cat $OUTPUT_FILES`
cat $INPUT_FILES | xargs driver --offline > $OUTPUT_FILENAME
