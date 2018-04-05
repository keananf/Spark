import pandas as pd


class ClusterSet:
    """
    Wrapper for a set of tweet clusters
    """
    def __init__(self, tweet_data, clusters, keywords):
        self.clusters = {}
        self.create_clusters(tweet_data, clusters, keywords)

    def create_clusters(self, tweet_data, clusters, keywords):
        """ Cluster the tweet data using keywords"""
        for i, c in enumerate(clusters):
            self.clusters[c] = Cluster(name=c)

        for idx, tweet in tweet_data.iterrows():
            tweet_text = tweet.text
            for i, keyword_list in enumerate(keywords):
                for keyw in keyword_list:
                    if keyw in tweet_text:
                        self.clusters[clusters[i]].insert(tweet)
                        break

        return

    def get(self, name):
        return self.clusters[name]


class Cluster:

    def __init__(self, name):
        self.name = name
        self.data = pd.DataFrame()

    def insert(self, tweet):
        self.data = self.data.append(tweet, ignore_index=True)