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
# BOPM benchmark for given parameters.
#
# usage: run-binoptions.sh after editing parameters

#!/bin/bash
MASTER="http://tigger-0.xeno.cl.cam.ac.uk:8000"
#SWROOT="/opt/skywriting"

for i in 1 2 3 4 5; do
    for c in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47; do
	for s in 1 2 3 4 5 6 7 8 9 10; do
	    echo "Running for c=$c, s=$s (repeat $i): "
	    export SPIN_DEGREE=$c
	    export SPIN_TIME=$s
	    export FOO=`date +%s`
	    $SWROOT/scripts/sw-flush-workers -m $MASTER -f
	    time $SWROOT/scripts/sw-job -m $MASTER -e $SWROOT/examples/Timespin/timespin.sw -i i${i}c${c}s${s} 2>&1
	done
    done
done

