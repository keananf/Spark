from src.spark import SparkInteraction
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType


schema = StructType([
    StructField("text", StringType(), True),
    StructField('entities', StructType([StructField('hashtags', ArrayType(StructType([StructField('indices', ArrayType(LongType(), True), True), StructField('text', StringType(), True)]), True), True)]), True),
    StructField('created_at', StringType(), True),
    StructField('favourite_count', LongType(), True),
    StructField('retweet_count', LongType(), True),
    StructField('id_str', StringType(), True),
    StructField('user', StructType([StructField('followers_count', LongType(), True), StructField('utc_offset', LongType(), True)]))
])


def load_datframe():
    spark = SparkInteraction('test', 'spark://pc5-035-l.cs.st-andrews.ac.uk:7077')
    sess = spark.session()
    df = sess.read.json("file:///cs/unique/ls99-kf39-cs5052/data/tweets/*.json", schema=schema)
