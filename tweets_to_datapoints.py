from influxdb import InfluxDBClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import math
import datetime as dt
import re
import cProfile

def clean(text):
    '''
    Removes twitter handles, RT handles, URLs and special characters
    '''
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r'@[\w]*', "", text)
    text = re.sub(r'RT @[\w]*', "", text)  
    text = re.sub(r'[^a-zA-Z#!:)(=)]', ' ', text)
    text = re.sub(' +', ' ', text)

    return text

# Para cada hora:

# - Pega o conjunto de tweets (OK)
# - Limpa os textos (OK)
# - Analisa os sentimentos (OK)
# - Calcula features dos tweets (OK)
# - Busca dados sobre bitcoins naquela hora (OK)
# - Calcula datapoint (vetor de feature) (OK)
# - Calcula a função Z(t) objetivo para aquele datapoint (OK)

# - Guarda tudo com Pickle

# Objetivo: 2160 datapoints

db_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'bitcoin_tweets')

# Building the analyzer takes the most time from vader
analyzer = SentimentIntensityAnalyzer()

def to_ns(ts):
    return int(ts) * 1000000000

def get_hourly_target_function(time):
    '''
    Get Z(t) for the hour starting at {time}.

    Z(t) is defined as follows:
        - If there is an increment in the closing price between t and t+1, Z(t) = 1
        - else, Z(t) = -1
    '''

    current_closing = get_hourly_btc_data(time)['close']
    next_closing = get_hourly_btc_data(time + dt.timedelta(hours=1))['close']

    return 1 if next_closing >= current_closing else -1

def get_hourly_btc_data(time):
    '''
    Get stats for starting at {time}

    --- Note that you need to use a time/timestamp that is actually present in the database ---
    '''
    results = db_client.query(f'SELECT * FROM bitcoin WHERE time = {to_ns(time.timestamp())}')

    return next(results.get_points())

def get_hourly_btc_features(time):
    '''
    Calculate hourly BTC features for {time}
    '''

    # [ close, high, low, open, volumeto ]
    data = get_hourly_btc_data(time)
    return [ data['close'], data['high'], data['low'], data['open'], data['volumeto'] ]

def get_tweets(time):
    '''
    Get 1 hour of tweets in InfluxDB from a given datetime.
    '''
    current_ts = to_ns(time.timestamp())
    next_ts = to_ns(time.timestamp() + 3600)

    results = db_client.query(f'SELECT * FROM tweet WHERE time >= {current_ts} and time < {next_ts}')

    data = []

    for r in results.get_points():
        data.append(r)

    return data

def get_hourly_clean_tweets(time):
    '''
    Get 1 hour of clean tweets starting from the given {time}
    '''
    data = get_tweets(time)
    
    return [ clean(tweet['text']) for tweet in data ]

def analyze_sentiment(text):
    '''
    Given a clean piece of text, analyze VaderSentiment scores for it
    '''
    return analyzer.polarity_scores(text)

def get_hourly_tweet_features(time):
    '''
    Given a time, analyze one hour of tweets starting from that time and calculate features.
    '''
    tweets = get_hourly_clean_tweets(time)
    n = len(tweets)

    sum_neu = 0
    sum_neg = 0
    sum_norm = 0
    sum_pos = 0

    for tweet in tweets:
        v = analyze_sentiment(tweet)

        sum_neu += v['neu']
        sum_neg += v['neg']
        sum_norm += v['compound']
        sum_pos += v['pos']

    # [ neu, norm, neg, pos, pol ]
    return [sum_neu/n , sum_neg/n, sum_norm/n, sum_pos/n, math.sqrt( sum_pos/n * sum_neg/n )]

def get_hourly_features(time):
    return get_hourly_btc_features(time) + get_hourly_tweet_features(time)

def get_all(start_time, data_points=24):
    '''
    Get {data_points} hourly datapoints starting at {start_time}.
    '''
    vt = []
    zt = []

    for date in (start_time + dt.timedelta(hours=n) for n in range(data_points)):
        print(f"---- Started {date} ----")
        vt.append(get_hourly_features(date))
        print("---- Calculating target ----")
        zt.append(get_hourly_target_function(date))

    return (vt, zt)

start_date = dt.datetime(2019, 1, 16, hour=15,tzinfo=dt.timezone.utc)

cProfile.run('get_all(start_date, data_points=3)')

# Save datapoints as a tuple
# datapoints = get_all(start_date, data_points=2160)
