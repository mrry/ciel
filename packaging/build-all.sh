#!/bin/bash

cd scripts
./check-deps.sh || exit 1
cd ..

mkdir -p logs store journal
./fetch-externals.sh || exit
./build-java.sh || echo "Failed to build Java bindings and examples"
which scalac && ./build-scala.sh || echo "Failed to build Scala bindings"
./gen-data.sh || echo "Failed to generate test data"
make -C src/c || echo "Make C bindings failed"
make -C src/csharp || echo "Make C# bindings failed"
