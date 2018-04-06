#!/usr/bin/env bash

COUNTER=0

readarray HOSTS < hosts.txt

for host in ${HOSTS[@]}
do
  test=$host
  status=$(ssh -o BatchMode=yes -o ConnectTimeout=5 $test echo ok 2>&1)

  if [[ $status == ok ]] ; then # If host alive, add to hosts array
    COUNTER=$((COUNTER+1))
    HOSTS+=($test)
    echo "Connected to: " $test " Count: " ${#HOSTS[@]}
    ssh $test 'cd ~/Downloads/spark-master; ./sbin/stop-slave.sh '
  fi
done
