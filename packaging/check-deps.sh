#!/bin/bash

echo "Checking mandatory dependencies..."
if type -P python > /dev/null
then
    echo "Python: found"
    if python check-python-deps.py
    then
	echo "All python deps OK"
    else
	echo "One or more mandatory Python dependency is missing"
	exit 1
    fi
else
    echo "Python: not found"
    exit 1
fi
echo "Checking non-essential dependencies..."
python check-python-optionals.py
if ! type -P lighttpd > /dev/null
then
    echo "lighttpd: not found. -l option won't work; CherryPy will be used as a data server"
else
    echo "lighttpd: found"
fi
if ! type -P m4 > /dev/null
then
    echo "m4: not found. -l option won't work; CherryPy will be used as a data server"
else
    echo "m4: found"
fi
if ! type -P ant > /dev/null
then
    echo "ant: not found. Won't be able to build Java bindings"
else
    echo "ant: found"
fi
if ! type -P javac > /dev/null
then
    echo "javac: not found. Won't be able to build Java bindings"
else
    echo "javac: found"
fi
if ! type -P gmcs > /dev/null
then
    echo "gmcs: not found. Won't be able to build C# bindings"
else
    echo "gmcs: found"
fi
if ! type -P gcc > /dev/null
then
    echo "gcc: not found. Won't be able to build C bindings"
else
    echo "gcc: found"
fi
if ! type -P pypy > /dev/null
then
    echo "pypy: not found. Won't be able to run SkyPy tasks"
else
    echo "pypy: found"
fi
if ! type -P protoc > /dev/null
then
    echo "protoc: not found. Won't be able to build protobuf bindings"
else
    echo "protoc: found"
fi
if ! test -e /usr/share/java/protobuf.jar
then
    echo "/usr/share/java/protobuf.jar: not found. Won't be able to use java protobuf bindings. Try installing libprotobuf-java."
else
    echo "/usr/share/java/protobuf.jar: found"
fi