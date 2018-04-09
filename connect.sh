#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "No spark url supplied"
    exit
fi

rm -f -- hosts.txt  # Delete hosts file if it exists, so we don't append hosts that don't exist

SPARK_URL=$1
FILE_PATH="/cs/unique/ls99-kf39-cs5052/data/tweets"
echo "Spark url: " $1

COUNTER=0
HOSTS=()

for i in $(seq -f "%03g" 1 150)
do
  host=pc2-${i}-l.cs.st-andrews.ac.uk
  status=$(ssh -o BatchMode=yes -o ConnectTimeout=5 $host echo ok 2>&1)

  if [[ $status == ok && ${#HOSTS[@]} -lt 20 ]] ; then # If host alive, add to hosts array
    if ssh $host test -e "$FILE_PATH" ; then
        COUNTER=$((COUNTER+1))
        HOSTS+=($host)
        echo "Connected to: " $host " Count: " ${#HOSTS[@]}
        echo -e "$host" >> hosts.txt
        ssh $host "cd $FILE_PATH; cd ~/Downloads/spark-master; ./sbin/start-slave.sh " $1
    fi
  elif [[ ${#HOSTS[@]} -eq 20 ]] ; then
    echo "Got enough hosts"
    break
  fi
done
