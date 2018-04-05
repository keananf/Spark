""" nlp.examples.example_sentiment.py

Example script for performing sentiment analysis on a single tweet file
In this example we:
 - load tweets from a previously generated file
 - Partition the tweets about Bitcoin using keywords
 - Apply sentiment to all tweets in the set
 - Tabulate the sentiment scores
 - Visualise the tweet sentiment volume bars
"""
from tweet_io import from_json
from classify.sentiment import Sentiment
from classify.cluster import ClusterSet
from clean.clean import remove_duplicates
from visualise.volume import sentiment_volume
from tabulate import tabulate
from pandas.io.json import to_json
import os


file_dir = os.path.dirname(os.path.realpath(__file__))

# Load Tweets as pandas data frame - these have already been cleaned
tweet_data = from_json(file_dir + '/sentiment_example.json')

# Remove duplicates
tweet_data = remove_duplicates(tweet_data)

# Cluster corresponding to btc tweets
clustered_tweet_data = ClusterSet(tweet_data,
                                  clusters=['bitcoin'],
                                  keywords=[['bitcoin', 'btc']])

btc_data = clustered_tweet_data.get('bitcoin').data

# Analyse Sentiment
btc_data = Sentiment(btc_data, rmv_stopwords=True).data

# Save btc data to separate file
to_json('example_btc_data.json', btc_data, orient='records')

# Tabulate the results
print(tabulate(btc_data, headers=btc_data.keys()))

# Plot the sentiment volume bars
sentiment_volume(btc_data, 1000)
