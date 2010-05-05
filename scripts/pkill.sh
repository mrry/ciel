#!/bin/bash
if [[ $1 == "" ]]; then
  echo "Usage : ./pkill.sh <process name>"
else
for i in `ps ax | grep $1| awk '{ print $1;}'`
do
  kill -9 $i
done
fi

