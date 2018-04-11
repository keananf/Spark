from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class Spark(object):
    """
    Wrapper class for spark interaction via either context or sql session
    """
    def __init__(self, appname='test', master='local'):
        self.appname = appname
        self.master = master
        self.conf = SparkConf().setAppName(self.appname).setMaster(self.master)

    def context(self):
        """
        Create a plain spark context at the url specified in self.master
        :return: SparkContext
        """
        return SparkContext(conf=self.conf)

    def session(self):
        """
        Create spark SQL Session - useful for structuring unstructured data, e.g. JSON
        :return: SQL Session
        """
        return SparkSession.builder.config(conf=self.conf).config('spark.jars', "/cs/home/kf39/Downloads/spark-nlp_2.11-1.5.0.jar").getOrCreate()

