from influxdb import InfluxDBClient
import pickle
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
            'text': tweet.text[:10]
        }
    }

    return [d]

tweets = []

with open(r'tweets\btc_tweets_2019-01-02', 'rb') as f:
    tweets = pickle.load(f)
    
tweets.sort(key=lambda t : t.timestamp)

print(tweets[0].timestamp)
print(tweets[ len(tweets) - 1 ].timestamp)

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'bitcoin_tweets')

num = 1
for t in tweets:
    print(f"Adding the {num} tweet ...")
    num+=1
    j = tweet_to_json(t)
    client.write_points(j)