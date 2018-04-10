import pyspark.sql.functions as fn
from pyspark.sql.types import StringType


lower_case = fn.udf(lambda text: fn.lower(text), StringType())

strip_urls = fn.udf(lambda text: text.str.replace(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', regex=True), StringType())

strip_whitespace = fn.udf(lambda text: text.str.replace('\n', '').replace('\r', '').replace('\t', '').replace('/r/', '').replace(r'\s{2,}', ' ', regex=True), StringType())

strip_amp = fn.udf(lambda text: text.str.replace('amp;', ''), StringType())

strip_punctuation = fn.udf(lambda text: text.str.replace(r'[\!\"\'\(\)\*\+\,\-\.\/\:\;\?\[\\\]\^\_\`\{\|\}\~]', '', regex=True), StringType())


def clean_tweets(df, col_name="text"):
    df = df.withColumn(col_name, lower_case(df[col_name]))
    df = df.withColumn(col_name, strip_urls(df[col_name]))
    df = df.withColumn(col_name, strip_whitespace(df[col_name]))
    df = df.withColumn(col_name, strip_amp(df[col_name]))
    df = df.withColumn(col_name, strip_punctuation(df[col_name]))
    return df



