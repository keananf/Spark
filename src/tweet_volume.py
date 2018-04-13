from src.spark import Spark
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
import pyspark.sql.functions as fs
import os
import pandas as pd
from src.settings import CRYPTO_DIR
from plotting import double_plot

schema = StructType([
    StructField("text", StringType(), True),
    StructField('entities', StructType([StructField('hashtags', ArrayType(StructType([StructField('indices', ArrayType(LongType(), True), True), StructField('text', StringType(), True)]), True), True)]), True),
    StructField('created_at', StringType(), True),
    StructField('favourite_count', LongType(), True),
    StructField('retweet_count', LongType(), True),
    StructField('id_str', StringType(), True),
    StructField('user', StructType([StructField('followers_count', LongType(), True), StructField('utc_offset', LongType(), True)]))
])

tweet_date_format = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
start_date = pd.Timestamp("2017-10-10")
end_date = pd.Timestamp("2017-11-21")


default_loc = "/cs/unique/ls99-kf39-cs5052/data/tweets/*.json"
default_spark_url = 'spark://pc2-060-l.cs.st-andrews.ac.uk:7077'


def create_session(spark_url):
    spark = Spark('test', spark_url)
    sess = spark.session()
    return sess


def load_dataframe(sess, file_location, schema=schema):
    df = sess.read.json("file://"+file_location, schema=schema)
    return df


def parse_timestamp(df):
    df2 = df.withColumn("timestamp", fs.to_timestamp("created_at", tweet_date_format))
    return df2


def aggregate_by_day(df):
    df2 = df.withColumn("date", fs.date_format("timestamp", "yyyy-MM-dd")).groupBy("date").count().toPandas()
    return df2


def load_crypto():
    cryptos = {}

    for subdir, dirs, files in os.walk(CRYPTO_DIR):
        for f in files:
            if f != "crix.csv":
                print("Loading %s" % f)
                fin = os.path.join(subdir, f)
                name_tokens = f.split("_")
                crypto = name_tokens[1]
                coin = pd.read_csv(fin, index_col=0)
                coin.index = pd.to_datetime(coin.index)
                coin = coin.loc[start_date:end_date]
                cryptos[crypto] = coin
    return cryptos


def load_crix():
    crix = pd.read_csv(os.path.join(CRYPTO_DIR, "crix.csv"), index_col=0)
    crix.index = pd.to_datetime(crix.index)
    crix = crix.loc[start_date:end_date]
    return crix


def overall_volume_price_correlation(date_parsed_df):
    all_tweets_agg = aggregate_by_day(date_parsed_df)  # Aggregate all tweet volume by date as pandas df

    crix = load_crix()  # Get crypto market index prices

    all_tweets_agg.dropna()
    all_tweets_agg.set_index('date', inplace=True)
    all_tweets_agg.index = pd.to_datetime(all_tweets_agg.index)

    crix_and_vol = crix
    crix_and_vol['volume'] = all_tweets_agg['count']
    crix_and_vol_nona = crix_and_vol.dropna()

    print("Market correlation to tweet volume: %.4f" % crix_and_vol.corr().price.volume)

    double_plot([crix_and_vol_nona.price, crix_and_vol_nona.volume], ['Index Price', 'Tweets'],
                ['Date', 'Price', 'Volume'], "Price vs Tweet Volume", crix_and_vol_nona.index.tolist())


def per_currency_correlation(date_parsed_df):
    coins = ['ETH', 'BTC', 'XMR', 'DASH', 'LTC', 'ETC', 'BCH']  #
    hash = {
        'ETH': ['%ethereum%', '%ether%', '%eth%'],
        'BTC': ['%bitcoin%', '%btc%', '%bitcoin%'],
        'XMR': ["%monero%", "%xmr%", "%monero%"],
        'DASH': ["%digital cash%", "%dash%", "%dash%"],
        'LTC': ["%litecoin%", "%ltc%", "%litecoin%"],
        'ETC': ["%ethereum classic%", "%etc%", "%eth classic%"],
        'BCH': ["%bitcoincash%", "%bch%", "%bitcoin cash%"]
    }
    cryptos = load_crypto()
    for coin in coins:
        df = date_parsed_df.dropna().dropDuplicates(["text"])
        filters = hash[coin]
        coin_tweets = df.filter(fs.lower(df['text']).like(filters[0]) | fs.lower(df['text'].like(filters[1])) | fs.lower(df['text'].like(filters[2])))
        coin_tweets_agg = aggregate_by_day(coin_tweets)

        coin_tweets_agg.dropna().set_index('date', inplace=True)
        coin_tweets_agg.index = pd.to_datetime(coin_tweets_agg.index)

        price_and_vol = cryptos[coin].loc[start_date:end_date]
        price_and_vol['price'] = price_and_vol.weightedAverage
        price_and_vol['volume'] = coin_tweets_agg['count']
        price_and_vol_nona = price_and_vol.dropna()

        print("Market correlation to tweet volume: %.4f" % price_and_vol.corr().price.volume)

        double_plot([price_and_vol_nona.price, price_and_vol_nona.volume], ['%s Price' % coin, 'Tweets'],
                    ['Date', 'Price', 'Volume'], "%s Price vs Tweet Volume" % coin, price_and_vol_nona.index.tolist())


def main():
    sess = create_session(default_spark_url)
    all_tweets = load_dataframe(sess, default_loc, schema)
    all_tweets_parsed = parse_timestamp(all_tweets)

    overall_volume_price_correlation(all_tweets_parsed)
    per_currency_correlation(all_tweets_parsed)


if __name__ == "__main__":
    main()
