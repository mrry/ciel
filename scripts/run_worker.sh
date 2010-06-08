#!/bin/bash
BASE=$(dirname $(readlink -f $0))/..
export PYTHONPATH=$PYTHONPATH:$BASE/src/python
PYTHON=${PYTHON:-python}

if [[ $1 == "" ]]; then
    HOST=`hostname -f`
else
    HOST=$1
fi

MASTER=${MASTER_HOST:-http://$HOST:9000}

WORKER_PORT=${WORKER_PORT:-9001}

export CLASSPATH=${BASE}/src/java/JavaBindings.jar:${BASE}/src/sw/json_simple-1.1.jar
${PYTHON} ${BASE}/src/python/skywriting/__init__.py --role worker --master ${MASTER} --port $WORKER_PORT
