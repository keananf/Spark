"""
Tools for loading data in panas df
"""

from pandas.io.json import json_normalize
import json
import csv


def from_json(file_path):
    """
    Load pandas df from json
    :param file_path:
    :return:
    """
    print("Loading tweets from file")
    # Each tweet is stored as a separate json object on each line of the json files
    frames = []
    with open(file_path) as f:
        for line in f.readlines():
            tweet_data = json.loads(line)
            frames.append(tweet_data[0])
    frames = json_normalize(frames)
    return frames
