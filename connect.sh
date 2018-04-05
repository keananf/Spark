for i in $(seq -f "%03g" 1 150)
do
  test = pc2-${i}-l.cs.st-andrews.ac.uk
  ssh test
done
