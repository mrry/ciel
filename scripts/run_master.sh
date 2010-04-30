#!/bin/sh
export PYTHONPATH=../src/python
PYTHON=python

${PYTHON} ../src/python/mrry/mercator/__init__.py --role master --port 9000
