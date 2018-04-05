from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext


remote_conf = SparkConf().setAppName('sparkTest').setMaster('spark://pc2-119-l.cs.st-andrews.ac.uk:7077')
local_conf = SparkConf().setAppName('sparkTest').setMaster('local')

sparkContext = SparkContext(conf=local_conf)
sparkSql = SparkSession.builder.config(conf=local_conf).getOrCreate()
