"""
General utility functions
"""

from datetime import datetime
from datetime import timedelta


def standardise_tweet_timestamp(tweet_df):
    """ Convert tweepy created_at attribute to standardised ms timestamp taking into account utc offset"""
    clean_timestamp_utc = datetime.strptime(tweet_df['created_at'].values[0], '%a %b %d %H:%M:%S +0000 %Y')
    # offset in hours from utc
    offset = tweet_df['user.utc_offset'].values[0]

    # account for offset from UTC using timedelta
    if offset:
        local_timestamp = clean_timestamp_utc + timedelta(seconds=int(offset))
    else:
        local_timestamp = clean_timestamp_utc

    # convert to am/pm format for easy reading
    final_timestamp = datetime.strftime(local_timestamp, '%Y-%m-%d %I:%M:%S %p')

    timestamp_ms = int(local_timestamp.strftime("%s")) * 1000
    return final_timestamp, timestamp_ms
