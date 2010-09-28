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
# GREP benchmark for given parameters.
#
# usage: run-grep.sh after editing parameters

MASTER=""
SWROOT="/opt/skywriting"
INPUT="<CIEL reference>"

for i in 1 2 3 4 5; do
    echo "Running iteration $i: "
    export FOO=`date +%s`
    export DATA_REF=$INPUT
    $SWROOT/scripts/sw-flush-workers -m $MASTER -f
    time $SWROOT/scripts/sw-job -m $MASTER -e $SWROOT/src/sw/grep.sw 2>&1
done
