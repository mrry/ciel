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
# Helper script to power on the local test cluster.
#
# usage: See srgcluster-boot.sh -h

# defaults
VERBOSE=0

# ---------------------------------------------
# option processing

while [ $# -gt 0 ]
do
  case $1
  in
    -f)
      if [[ $2 = '--' ]]; then
         # STDIN input
         SOURCE=''
      else
         # Cluster definition file
         SOURCE=$2
      fi
      shift 2
    ;;

    -v)
      VERBOSE=1
      shift 1
    ;;

    -h|*)
      echo "Turns the local test cluster machines' power on."
      echo "usage: srgcluster-boot [-f cluster-file|-v]"
      echo ""
      echo "-f: the file listing the machines in the cluster, one per line."
      echo "    If '--' is passed, STDIN is assumed."
      echo "-v: verbose mode (don't surpress output from xenuse)"
      shift 1
      exit 0
    ;;
  esac
done

# ---------------------------------------------
# main script

I=0
cat $SOURCE | while myLine=`line`
do
    echo -n "Turning on instance $I: "
    if [[ $VERBOSE -eq 1 ]]; then
	echo $myLine | cut -d \- -f 1 | xargs xenuse --on 
    else
	echo $myLine | cut -d \- -f 1 | xargs xenuse --on 1>&2 2>/dev/null
    fi
    echo $myLine
    I=`expr $I + 1`
done

exit 0
