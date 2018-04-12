from spark import Spark
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType
import pyspark.sql.functions as fs
import os
import pandas as pd
from settings import CRYPTO_DIR
from plotting import double_plot
from functools import reduce

# JSON tweet schema for parsing into dataframes
schema = StructType([
    StructField("text", StringType(), True),
    StructField('entities', StructType([StructField('hashtags', ArrayType(StructType([StructField('indices', ArrayType(LongType(), True), True), StructField('text', StringType(), True)]), True), True)]), True),
    StructField('created_at', StringType(), True),
    StructField('favourite_count', LongType(), True),
    StructField('retweet_count', LongType(), True),
    StructField('id_str', StringType(), True),
    StructField('user', StructType([StructField('followers_count', LongType(), True), StructField('utc_offset', LongType(), True)]))
])

# Date information regarding tweets
tweet_date_format = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
start_date = pd.Timestamp("2017-10-10")
end_date = pd.Timestamp("2017-11-21")

# Location of all tweets in a globally accessible location
default_loc = "/home/keanan/Documents/CS5052/Practical/tweets/*.json" #"/cs/unique/ls99-kf39-cs5052/data/tweets/*.json"

# Spark master location
default_spark_url = 'local'

# Crypto currency information
coins = ['ETH', 'BTC']
hash = {
    'BCH': ['%bitcoin cash%', '%bch%'],
    'BTC': ['%bitcoin%', '%btc%'],
    'DASH': ['%dash%'],
    'ETC': ['%ethereum classic%', '%etc%'],
    'ETH': ['%ethereum%', '%ether%', '%eth%'],
    'LTC': ['%litecoin%', '%ltc%', '%lite%'],
    'XMR': ['%monero%', '%xmr%']
}
crypto_files = {
    'BCH': "KRAKEN_BCH_EUR.csv",
    'BTC': "KRAKEN_BTC_EUR.csv",
    'DASH': "KRAKEN_DASH_EUR.csv",
    'ETC': "KRAKEN_ETC_EUR.csv",
    'ETH': "KRAKEN_ETH_EUR.csv",
    'LTC': "KRAKEN_LTC_EUR.csv",
    'XMR': "KRAKEN_XMR_EUR.csv",
}

def create_session(spark_url):
    """ Creates a new spark session at the url
    :param spark_url: the url to start spark at
    :return: the new spark session
    """
    spark = Spark('test', spark_url)
    sess = spark.session()
    return sess


def load_dataframe(sess, file_location, schema=schema):
    """ Loads in a dataframe from all files at the given location

    :param sess: the spark session
    :param file_location: the file location
    :param schema: the JSON schema used when parsing the data
    :return: the parsed data-frame
    """
    return sess.read.json("file://"+file_location, schema=schema)


def parse_timestamp(df):
    """ Parses the created_at column into a timestamp of a particular format.

    :param df: the df to modify
    :return: the same df but with an additional timestamp column
    """
    return df.withColumn("timestamp", fs.to_timestamp("created_at", tweet_date_format))


def aggregate_by_day(df):
    """ Aggregate all tweets according to the day they were posted
    This is called after parse_timestamp()
    :param df: the df to aggregate
    :return: the df with a new 'date' column, containing the day each tweet was posted
    """
    return df.withColumn("date", fs.date_format("timestamp", "yyyy-MM-dd")).groupBy("date").count().toPandas()


def load_crypto(coin=""):
    """ Loads in the crypto price data for the given coin

    :param coin: the coin to load the data for
    :return: a df containing this coin's price info, on a half-hourly basis.
    """
    # Load in all related crypto price data. Then, parse the index column as timestamps
    coin_price_data = pd.read_csv(os.path.join(CRYPTO_DIR, crypto_files[coin]), index_col=0)
    coin_price_data.index = pd.to_datetime(coin_price_data.index)
    return coin_price_data.loc[start_date:end_date]


def load_crix():
    """ Loads in average overall price information for major crypto currencies

    :return: the price information for major crypto currencies by datetime
    """
    crix = pd.read_csv(os.path.join(CRYPTO_DIR, "crix.csv"), index_col=0)
    crix.index = pd.to_datetime(crix.index)
    crix = crix.loc[start_date:end_date]
    return crix


def overall_volume_price_correlation(date_parsed_df):
    """ Create the overall tweet volume / market correlation, and graph.

    :param date_parsed_df: the overall df of all tweets, with parsed date column as an index
    """
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


def find_all_currency_correlations(date_parsed_df):
    """ Calculate the market / tweet volume correlations for each crypto currency

    :param date_parsed_df: the overall df of all tweets, with parsed date column as an index
    """
    # For each coin, calculate the market correlation with tweet volume, as well as plot it.
    for coin in coins:
        find_currency_correlation(date_parsed_df, coin)


def find_currency_correlation(date_parsed_df, coin=""):
    """ Calculates the market / tweet volume correlation for the given crypto currency

    :param date_parsed_df: the overall df of all tweets, with parsed date column as an index
    :param coin: the coin
    """
    # Remove all duplicates from the df of tweets
    df = date_parsed_df#.dropna().dropDuplicates(["text"])

    # Find all tweets pertaining to this particular coin
    filters = hash[coin]
    coin_tweets = df.filter(reduce(lambda f1, f2: f1 | f2, map(lambda f: fs.lower(df['text']).like(f), filters)))

    # Set the index of the df to be the parsed date column for all tweets relating to this coin
    coin_tweets = aggregate_by_day(coin_tweets)
    coin_tweets = coin_tweets.dropna()
    coin_tweets.set_index('date', inplace=True)
    coin_tweets.index = pd.to_datetime(coin_tweets.index)

    # Combine the coin price data with the data-frame representing the volume of coin tweets per day
    price_and_vol = load_crypto(coin)
    price_and_vol['price'] = price_and_vol.weightedAverage
    price_and_vol['volume'] = coin_tweets['count']
    price_and_vol = price_and_vol.dropna()

    # Print final results, and plot the correlation
    print("Market correlation to tweet volume: %.4f" % price_and_vol.corr().price.volume)
    double_plot([price_and_vol.price, price_and_vol.volume], [coin + ' Price', 'Tweets'],
                ['Date', 'Price', 'Volume'], coin + " Price vs Tweet Volume", price_and_vol.index.tolist())


def main():
    sess = create_session(default_spark_url)
    all_tweets = load_dataframe(sess, default_loc, schema)
    all_tweets_parsed = parse_timestamp(all_tweets)

    overall_volume_price_correlation(all_tweets_parsed)
    find_all_currency_correlations(all_tweets_parsed)


if __name__ == "__main__":
    main()
