# -*- coding: utf-8 -*-
import tweepy
import json
import pandas as pd
from pandas.io.json import json_normalize, to_json
from clean.clean import clean_tweets
from utils.utils import standardise_tweet_timestamp


class TwitterScraper:

    def __init__(self):
        self.api = None

    def authorise(self, api_key, api_secret, access_token_key, access_token_secret):
        """
        Authorise access to Twitter API
        :param api_key:
        :param api_secret:
        :param access_token_key:
        :param access_token_secret:
        :return:
        """
        # Set up the authorisation
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token_key, access_token_secret)
        self.api = tweepy.API(auth)
        return

    def get_tweets(self, q, fields, save_file, **kwargs):
        """ Query the Twitter api and save tweets to json """
        out_file = open(save_file, 'a')

        for tweet in tweepy.Cursor(self.api.search, q=q, **kwargs).items():
            print(tweet.text)
            if fields:
                tweet_df = json_normalize(tweet._json)
                new_df = pd.DataFrame()
                for field_name in fields:
                    if field_name in tweet_df.keys():
                        new_df[field_name] = tweet_df[field_name]
                        new_df['timestamp'], new_df['timestamp_ms'] = standardise_tweet_timestamp(tweet_df)
                    else:
                        print("field: " + field_name + " not in tweet data")

                # clean the tweet data
                new_df = clean_tweets(new_df)

                # convert back to json
                to_json(out_file, new_df, orient='records')
                out_file.write('\n')
            else:
                out_file.write(json.dumps(tweet._json))
                out_file.write("\n")

        out_file.close()
        return
