"""
Tools for cleaning tweet data prior to sentiment analysis
"""
import re
import pandas as pd

def lower_case(text):
    return text.lower()


def strip_urls(text):
    return re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text, flags=re.MULTILINE)


def strip_whitespace(text):
    stripped = text.replace('\n', '').replace('\r', '').replace('\t', '').replace('/r/', '').strip()
    return re.sub(r'\s{2,}', ' ', stripped)


def strip_amp(text):
    return text.replace('amp;', '')


def strip_ellipsis(text):
    return text.replace('…', '')


def strip_punctuation(text):
    return re.sub(r'[\!\"\'\(\)\*\+\,\-\.\/\:\;\?\[\\\]\^\_\`\{\|\}\~]', '', text)


def remove_duplicates(data):
    """
    Remove duplicate tweets based on text
    :param data:
    :return:
    """
    unique_tweets = []
    to_remove_idx = []
    for idx, tweet in data.iterrows():
        tweet_text = tweet.text
        if tweet_text not in unique_tweets:
            unique_tweets.append(tweet_text)
        else:
            to_remove_idx.append(idx)

    data = data.drop(data.index[to_remove_idx])
    return data


def clean_tweets(data):
    """
    Clean the tweet data
    :param data: Pandas dataframe of tweet data
    :return: Cleaned version
    """
    data['text'] = data['text'].apply(lower_case)
    data['text'] = data['text'].apply(strip_urls)
    data['text'] = data['text'].apply(strip_whitespace)
    data['text'] = data['text'].apply(strip_amp)
    data['text'] = data['text'].apply(strip_ellipsis)
    data['text'] = data['text'].apply(strip_punctuation)
    return data
