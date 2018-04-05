from scrape.twitter import TwitterScraper

# Twitter access keys
api_key = ""
api_secret = ""
access_token_key = ""
access_token_secret = ""

# Generate a scraper
ts = TwitterScraper()
ts.authorise(api_key, api_secret, access_token_key, access_token_secret)

# Get tweets and save to file
fields = ['text',
          'entities.hashtags',
          'user.followers_count',
          'created_at',
          'user.utc_offset',
          'favorite_count',
          'retweet_count',
          'id_str',
          'user.verified']

ts.get_tweets(q="#bitcoin", fields=fields, save_file='test.json')
