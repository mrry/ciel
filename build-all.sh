#!/bin/bash

./fetch-externals.sh || exit
./build-java.sh || echo "Failed to build Java bindings and examples"
./gen-data.sh || echo "Failed to generate test data"
make -C src/c/src loader || echo "Make C bindings failed"
make -C src/csharp || echo "Make C# bindings failed"