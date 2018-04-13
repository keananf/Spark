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
        return SparkSession.builder.config(conf=self.conf)\
            .config('spark.jars', "/cs/unique/ls99-kf39-cs5052/sparknlp.jar")\
            .config("spark.driver.memory", "3g")\
            .getOrCreate()

