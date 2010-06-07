#!/bin/bash
export PYTHONPATH=$PYTHONPATH:../src/python
PYTHON=${PYTHON:-python}

HOST=`hostname -f`

MASTER_HOST=${MASTER_HOST:-http://$HOST:9000}

${PYTHON} ../src/python/skywriting/lang/cluster.py --master=${MASTER_HOST} $1
