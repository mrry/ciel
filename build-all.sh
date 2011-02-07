#!/bin/bash

./fetch-externals.sh
./build-java.sh
./gen-data.sh
make -C src/c/src loader
make -C src/csharp