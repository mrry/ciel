#!/bin/bash
# Copyright (c) 2010 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# ----
#
# Evaluation/benchmarking helper script. Performs iterative runs of the
# WORDCOUNT benchmark on Hadoop.
#
# usage: run-wordcount-hadoop.sh after editing parameters

HADOOP_HOME="/usr/lib/hadoop"
HADOOP_VER="0.20.2+320"
INPUT="<HDFS reference>"

for i in 1 2 3 4 5; do
    echo "Running iteration $i: "
    time hadoop jar $HADOOP_HOME/hadoop-$HADOOP_VER-examples.jar wordcount $INPUT /user/root/wc-out/run$i "A27"
done
