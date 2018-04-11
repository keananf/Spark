import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace


lower_case = lambda text: fn.lower(text)

strip_urls = lambda text: regexp_replace(text, r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '')

strip_whitespace = lambda text: regexp_replace(text, '[\n\r\t/r/\s{2,}]+', '')

strip_amp = lambda text: regexp_replace(text, 'amp;', '')

strip_punctuation = lambda text: regexp_replace(text, r'[\!\"\'\(\)\*\+\,\-\.\/\:\;\?\[\\\]\^\_\`\{\|\}\~]', '')


def clean_tweets(df, col_name="text"):
    # Lowercase
    df = df.withColumn(col_name, fn.lower(df[col_name]))
    # Strip urls
    df = df.withColumn(col_name, regexp_replace(df[col_name], r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ''))
    # Strip ampersand
    df = df.withColumn(col_name, regexp_replace(df[col_name], 'amp;', ''))
    # Strip punctuation
    df = df.withColumn(col_name, regexp_replace(df[col_name], r'[\!\"\'\(\)\*\+\,\-\.\/\:\;\?\[\\\]\^\_\`\{\|\}\~]', ''))
    # Strip whitespace
    df = df.withColumn(col_name, regexp_replace(df[col_name], '[\n|\r|\t]+', ''))
    df = df.withColumn(col_name, regexp_replace(df[col_name], '[\s]{2,}', ''))
    return df



