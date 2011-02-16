#!/bin/bash

cd scripts
./check-deps.sh || exit 1
cd ..

mkdir -p logs store journal
./fetch-externals.sh || exit
make -C src/protoc || echo "Failed to build protocol buffers"
./build-java.sh || echo "Failed to build Java bindings and examples"
./gen-data.sh || echo "Failed to generate test data"
make -C src/c/src loader || echo "Make C bindings failed"
make -C src/csharp || echo "Make C# bindings failed"
