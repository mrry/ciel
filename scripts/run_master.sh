#!/bin/bash
BASE=$(dirname $(readlink -f $0))/..
export PYTHONPATH=$PYTHONPATH:$BASE/src/python
PYTHON=python

# Sensible defaults:
if [[ $MASTER_PORT == "" ]]; then
    MASTER_PORT=8000
fi

if [[ $REL_BLOCK_LOCATION == "" ]]; then
    REL_BLOCK_LOCATION="store/"
fi

${PYTHON} $BASE/src/python/skywriting/__init__.py --role master --port $MASTER_PORT --staticbase $BASE/src/js/skyweb/ --lighttpd-conf $BASE/src/python/skywriting/runtime/lighttpd.conf -j $BASE/journal/ -b $BASE/$REL_BLOCK_LOCATION -T ciel-process-aaca0f5eb4d2d98a6ce6dffa99f8254b $*
