import matplotlib.pyplot as plt
import numpy as np


def sentiment_volume(data, window):
    """
    Plot the twitter sentiment information as volume bars

    :param data: dataframe of tweets and associated sentiment data
    :param window: time window in ms
    :return:
    """
    # TODO: This doesnt give particularly nice output atm


    # First find the maximum and minimum timestamps
    min_time = min(data['timestamp_ms'])
    max_time = max(data['timestamp_ms'])
    number_of_tweets = len(data)
    bar_width = (int(max_time) - int(min_time))/window

    times = np.linspace(int(min_time), int(max_time), bar_width)
    times_scaled = np.linspace(0, int(max_time) - int(min_time), bar_width)

    volume = np.zeros(len(times))
    sentiment = np.zeros(len(times))

    for index, tweet in data.iterrows():
        for t in range(0, len(times)-1):
            if times[t] <= int(tweet['timestamp_ms']) < times[t+1]:
                sentiment[t] += float(tweet['compound'])
                volume[t] += 1
                continue

    sentiment = sentiment/volume
    sentiment = np.nan_to_num(sentiment)
    mask1 = sentiment < 0.0
    mask2 = sentiment >= 0.0

    plt.figure()
    plt.bar(times_scaled[mask1], volume[mask1], width=1000, color='red', label='Neg')
    plt.bar(times_scaled[mask2], volume[mask2], width=1000, color='green', label='Pos')
    plt.xlabel('Timestamps ms')
    plt.ylabel('Volume')
    plt.legend(loc='best')
    plt.show()
    return
