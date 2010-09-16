#!/usr/bin/awk -f

# This program takes the output of `pywebgraph-2.72 -t csv | sort -g`
# (i.e. one edge per line, sorted in general numeric order so that edges from
# the same ID are grouped) and turns it into tab-separated format for
# consumption by the PageRank(Partition|Init)Task.
#
# The output format is:
# <nodeID> <TAB> <otherNodeID0> <TAB> <otherNodeID1> <TAB> ... <otherNodeIDk> <NEWLINE>
#
# Derek Murray, 16th September 2010.

BEGIN {
    FS=","
    i=-1
}

{
    if (i != $1) {
	if (i != -1) {
	    print line
	}
	i = $1
	line = $1 "\t" $2
    } else {
	line = line "\t" $2
    }
}

END {
    if (i != -1) {
	print line
    }
}