'''
Lets start importing a 3 month period ...
01/01/2019 - 30/03/2019
'''
from influxdb import InfluxDBClient
import twitterscraper
import pickle
import time
import datetime as dt
import random

def convert_timestamp(ts):
    '''
    Converts seconds precision timestamps to nanoseconds precision.

    Also adds a random number to make sure tweets in the same second are unique.
    '''
    return ts * 1000000000 + random.randint(1,1000000000)

def tweet_to_json(tweet):
    d = {
        'measurement': 'tweet',
        'tags': {
            'likes': tweet.likes,
            'replies': tweet.replies,
            'retweets': tweet.retweets
        },
        'time': convert_timestamp(tweet.timestamp_epochs),
        'fields': {
            'text': tweet.text
        }
    }

    return [d]

start_date = dt.date(2019, 4, 24)

# crawl_dates = [ dt.date(2019, 3, 17), dt.date(2019, 3, 18), dt.date(2019, 3, 24), dt.date(2019, 3, 26) ]

db_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'bitcoin_tweets')

for date in (start_date + dt.timedelta(n) for n in range(50)):
# for date in crawl_dates:

    start_time = time.time()

    with open('logs.txt', 'a+') as f:
        f.write(f"Starting on day {date} ...\n")
        tweets = twitterscraper.query_tweets("bitcoin OR Bitcoin OR btc OR BTC OR Btc", begindate=date, enddate=date + dt.timedelta(1), lang='en')

        f.write(f"---- Found {len(tweets)} tweets. ----\n")
        f.write(f"---- Took {time.time() - start_time} seconds ----\n")

    for t in tweets:
        j = tweet_to_json(t)
        db_client.write_points(j)

    

