#!/bin/sh
BASE=$(dirname $(readlink -f $0))/..
export PYTHONPATH=$PYTHONPATH:$BASE/src/python
PYTHON=python

${PYTHON} $BASE/src/python/skywriting/__init__.py --role master --port 9000 --staticbase $BASE/src/js/skyweb/ --lighttpd-conf $BASE/src/python/skywriting/runtime/lighttpd.conf
