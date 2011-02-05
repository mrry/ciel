#!/bin/bash

./fetch-externals.sh
ant build-tests build-examples -Ddir.build build -Ddir.dist dist