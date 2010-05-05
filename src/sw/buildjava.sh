#!/bin/bash
javac Splitter.java -cp ../java/JavaBindings.jar:./json_simple-1.1.jar
jar -cf ~/public_html/magic.jar Splitter.class