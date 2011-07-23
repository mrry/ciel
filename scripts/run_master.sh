#!/bin/bash

# Sensible defaults:
if [[ $MASTER_PORT == "" ]]; then
    MASTER_PORT=8000
fi

if [[ $REL_BLOCK_LOCATION == "" ]]; then
    REL_BLOCK_LOCATION="store/"
fi


if [ "${0#/}" = "${0}" ]
then
    me=$(pwd)/$0
else
    me=$0
fi

# Figure out whether this is running in-place in the source directory
# or in an installed version
install_prefix=${me%bin/run_master.sh}
if [ "$install_prefix" = "$me" ]
then
    # It's a source install
    BASE=$(pwd)
    my_python_path="$BASE/src/python"
    sw_master=${BASE}/scripts/sw-master
    staticbase=${BASE}/src/js/skyweb/
    lighttpd_conf=${BASE}/src/python/skywriting/runtime/lighttpd.conf
else
    # Running from an installed version
    if [ "$install_prefix" != "/" ] && [ "$install_prefix" != "/usr" ] && [ "$install_prefix" != "/usr/local" ]
    then
	PYTHONVER=$(python --version 2>&1 | cut -d' ' -f 2 | cut -d'.' -f 1,2)
	my_python_path=$install_prefix/lib/python${PYTHONVER}/site-packages
    else
	my_python_path=""
    fi
    sw_master=${install_prefix}/bin/sw-master
    staticbase=${install_prefix}/share/ciel/skyweb/
    lighttpd_conf=${install_prefix}/share/ciel/lighttpd.conf
fi
if ! [ -z "$my_python_path" ]
then
    if [ -z "$PYTHONPATH" ]
    then
	export PYTHONPATH=$my_python_path
    else
	PYTHONPATH=${PYTHONPATH}:$my_python_path
    fi
fi

ABS_BLOCK_LOCATION="$BASE/$REL_BLOCK_LOCATION"

if [ ! -d "$ABS_BLOCK_LOCATION" ]; then
  mkdir -p "$ABS_BLOCK_LOCATION"
fi

LIGHTTPD_BIN=`which lighttpd`
if [ "$LIGHTTPD_BIN" != "" ]; then
  EXTRA_CONF="${EXTRA_CONF} --lighttpd-conf ${lighttpd_conf}"
fi

${sw_master} --role master --port $MASTER_PORT --staticbase "$staticbase" -b "$ABS_BLOCK_LOCATION" -T ciel-process-aaca0f5eb4d2d98a6ce6dffa99f8254b ${EXTRA_CONF} $*
