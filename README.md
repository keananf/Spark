# Spark

## Setup

This project uses a number of python packages, so to run the system, do the following:

```bash
pip install jupyter pyspark matplotlib pandas numpy

# Assuming in project path
export PYTHONPATH=$PYTHONPATH:`pwd`

jupyter notebook  # Start notebook server to load the notebook files
```

## Spark Setup

If you want to use a spark cluster, start a Spark Master process and get the spark master URL (e.g. spark://pc5-035-l.cs.st-andrews.ac.uk:7077)
then pass this to the spark connector bash script (you need to be in the comp sci lab for this to work).

```bash
./connect.sh spark://pc5-035-l.cs.st-andrews.ac.uk
```

If you don't want to run a cluster, the notebooks are set to use local machine by default anyway.