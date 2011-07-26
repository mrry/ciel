#!/bin/bash -e

if [ "${0#/}" = "${0}" ]
then
    me=$(pwd)/$0
else
    me=$0
fi

install_prefix=${me%bin/run_job.sh}
if [ "$install_prefix" = "$me" ]
then
    BASE=$(pwd)
    my_python_path="$BASE/src/python"
    sw_start_job=${BASE}/scripts/sw-start-job
else
    PYTHONVER=$(python --version 2>&1 | cut -d' ' -f 2 | cut -d '.' -f1,2)
    pprefix=$install_prefix/lib/python${PYTHONVER}/site-packages
    if [ "$install_prefix" != "/" ] && [ "$install_prefix" != "/usr" ] && [ "$install_prefix" != "/usr/local" ]
    then
	my_python_path="${pprefix}"
    else
	my_python_path=""
    fi
    sw_start_job=${install_prefix}/bin/sw-start-job
fi
if ! [ -z "$my_python_path" ]
then
    if [ -z "$PYTHONPATH" ]
    then
	export PYTHONPATH="$my_python_path"
    else
	PYTHONPATH="${PYTHONPATH}:$my_python_path"
    fi
fi

${sw_start_job} $*
