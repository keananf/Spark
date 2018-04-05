from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('sparkTest').setMaster('spark://pc2-119-l.cs.st-andrews.ac.uk:7077')
sc = SparkContext(conf=conf)
data = range(10)
dist_data = sc.parallelize(data)
print(dist_data.reduce(lambda a, b: a+b))
