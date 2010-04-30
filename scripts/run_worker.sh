#!/bin/bash
export PYTHONPATH=$PYTHONPATH:../src/python
PYTHON=${PYTHON:-python}

MASTER=${MASTER_HOST:-http://click.local:9000}

WORKER_PORT=${WORKER_PORT:-9001}

export CLASSPATH=${PWD}/../src/java/JavaBindings.jar:${PWD}/../src/sw/json_simple-1.1.jar
${PYTHON} ../src/python/mrry/mercator/__init__.py --role worker --master ${MASTER} --port $WORKER_PORT
