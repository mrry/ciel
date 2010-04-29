#!/bin/bash
export PYTHONPATH=../src/python
PYTHON=python

MASTER=${MASTER_HOST:-http://click.local:9000}

export CLASSPATH=${PWD}/../java/JavaBindings.jar:$(PWD)/../sw/json_simple-1.1.jar
${PYTHON} ../src/python/mrry/mercator/__init__.py --role worker --master ${MASTER} --port 9001
