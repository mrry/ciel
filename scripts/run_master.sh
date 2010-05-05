#!/bin/sh
BASE=$(dirname $(readlink -f $0))/..
export PYTHONPATH=$PYTHONPATH:$BASE/src/python
PYTHON=python2.5

${PYTHON} $BASE/src/python/mrry/mercator/__init__.py --role master --port 9000
