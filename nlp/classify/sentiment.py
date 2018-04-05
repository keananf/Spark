from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
from nltk.corpus import stopwords


def remove_stop_words(text):
    stop = set(stopwords.words('english'))
    split_text = text.split()
    new_text = [i for i in split_text if i not in stop]

    return " ".join(new_text)


class Sentiment:
    """
    Generic sentiment classifier class
    """
    def __init__(self, tweet_data, rmv_stopwords=False):
        self.sid = SentimentIntensityAnalyzer()
        self.data = self.analyse_sentiment(tweet_data, rmv_stopwords)

    def analyse_sentiment(self, data, rmv_stopwords):
        """
        Calculate sentiment scores for each tweet in data using VADER
        :param data: Pandas dataframe of tweets
        :param rmv_stopwords: wether or not to remove stop words from the text
        :return: dataframe of sentiment and tweet data
        """
        sentiment_data = pd.DataFrame()

        for index, tweet in data.iterrows():
            tweet_text = tweet['text']
            if rmv_stopwords:
                tweet_text = remove_stop_words(tweet_text)

            tweet_sentiment_data = pd.Series(self.sid.polarity_scores(tweet_text))

            sentiment_data = sentiment_data.append(tweet_sentiment_data, ignore_index=True)

        data = pd.concat([data, sentiment_data], axis=1)

        return data
