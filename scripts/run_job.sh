#!/bin/bash
export PYTHONPATH=../src/python
PYTHON=${PYTHON:-python}

MASTER_HOST=${MASTER_HOST:-http://click.local:9000}

${PYTHON} ../src/python/mrry/mercator/cloudscript/interpreter/cluster.py $1 ${MASTER_HOST}
