#!/bin/bash

rm -rf build/scala
mkdir -p build/scala

rm -rf build/examples/scala-tests
mkdir -p build/examples/scala-tests

find src/scala -name "*.scala" | xargs scalac -cp dist/skywriting.jar:ext/google-gson-1.6/gson-1.6.jar -d build/scala

cd build/scala
find . -name "*.class" | xargs jar uf ../../dist/skywriting.jar
cd ../../

find examples/scala-tests/src -name "*.scala" | xargs scalac -cp dist/skywriting.jar:ext/google-gson-1.6/gson-1.6.jar -d build/examples/scala-tests

cd build/examples/scala-tests
find . -name "*.class" | xargs jar cf ../../../dist/scala-tests.jar
cd ../../
