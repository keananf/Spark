SPARK_URL=$1
echo "Spark url: " $1
COUNTER=0
HOSTS=()
for i in $(seq -f "%03g" 1 150)
do
  test=pc2-${i}-l.cs.st-andrews.ac.uk
  status=$(ssh -o BatchMode=yes -o ConnectTimeout=5 $test echo ok 2>&1)

  if [[ $status == ok && ${#HOSTS[@]} -lt 20 ]] ; then # If host alive, add to hosts array
    COUNTER=$((COUNTER+1))
    HOSTS+=($test)
    echo "Connected to: " $test " Count: " ${#HOSTS[@]}
    ssh $test 'cd ~/Downloads/spark-master; ./sbin/start-slave.sh ' $1
    echo "Starting slave on host"
  elif [[ ${#HOSTS[@]} -eq 20 ]] ; then
    echo "Got enough hosts"
    break
  fi
done
