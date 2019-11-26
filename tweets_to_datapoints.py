from influxdb import InfluxDBClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import math
import datetime as dt
import re
import cProfile
import pickle

class NoTweetsException(Exception):
    pass

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
# - Guarda tudo com Pickle (OK)

# ------------------------
# Teste: coletar market data no mesmo periodo do paper e tentar obter resultados iguais

# Objetivo: 2160 datapoints

db_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'bitcoin_tweets')

# Building the analyzer takes the most time from vader
analyzer = SentimentIntensityAnalyzer()

def to_ns(ts):
    return int(ts) * 1000000000

def get_target_function(time, h=1):
    '''
    Get Z(t) for the hour starting at {time}.

    Z(t) is defined as follows:
        - If there is an increment in the closing price between t and t+1, Z(t) = 1
        - else, Z(t) = -1
    '''

    current_closing = get_btc_data(time, h)['close']
    next_closing = get_btc_data(time + dt.timedelta(hours=h), h)['close']

    return 1 if next_closing >= current_closing else -1

def get_btc_data(time, hours=1):
    '''
    Get stats for starting at {time}

    --- Note that you need to use a time/timestamp that is actually present in the database ---
    '''
    next_time = time + dt.timedelta(hours=hours)
    results = db_client.query(f'SELECT * FROM bitcoin WHERE time >= {to_ns(time.timestamp())} and time < {to_ns(next_time.timestamp())}')
    results = list(results.get_points())

    print(time, hours)
    
    # Aggregate results if greater than 1
    if len(results) > 1:
        data = {}
        data['close'] = results[-1]['close']
        data['open'] = results[0]['open']
        data['high'] = max(results, key= lambda r: r['high'])['high']
        data['low'] = min(results, key= lambda r: r['low'])['low']
        data['volumeto'] = sum(r['volumeto'] for r in results)

        return data

    return results[0]

def get_btc_features(time, hours=1):
    '''
    Calculate hourly BTC features for {time}
    '''

    # [ close, high, low, open, volumeto ]
    data = get_btc_data(time, hours)
    return [ data['close'], data['high'], data['low'], data['open'], data['volumeto'] ]

def get_tweets(time, hours=1):
    '''
    Get {hours} of tweets in InfluxDB from a given datetime.
    '''
    current_ts = to_ns(time.timestamp())
    next_ts = to_ns(time.timestamp() + (3600 * hours))

    results = db_client.query(f'SELECT * FROM tweet WHERE time >= {current_ts} and time < {next_ts}')

    data = []

    for r in results.get_points():
        data.append(r)

    return data

def get_clean_tweets(time, hours=1):
    '''
    Get {hours} of clean tweets starting from the given {time}
    '''
    data = get_tweets(time, hours)
    
    return [ clean(tweet['text']) for tweet in data ]

def analyze_sentiment(text):
    '''
    Given a clean piece of text, analyze VaderSentiment scores for it
    '''
    return analyzer.polarity_scores(text)

def get_tweet_features(time, hours=1):
    '''
    Given a time, analyze {hours} of tweets starting from that time and calculate features.
    '''
    tweets = get_clean_tweets(time, hours)
    n = len(tweets)

    if n == 0:
        raise NoTweetsException("No tweets found for this hour")

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

def get_features(time, hours=1):
    return get_btc_features(time, hours) + get_tweet_features(time, hours)

def get_all(start_time, data_points=24, daily=False):
    '''
    Get twitter, market and twitter + market {data_points} hourly datapoints starting at {start_time}.
    '''
    period = 1
    if daily:
        period = 24

    twitter_vt = []
    twitter_zt = []

    mixed_vt = []
    mixed_zt = []

    market_vt = []    
    market_zt = []

    for date in (start_time + dt.timedelta(hours=n*period) for n in range(data_points)):
        print(f"---- Started {date} ----")

        target = get_target_function(date, period)

        market_features = get_btc_features(date, period)
        market_vt.append(market_features)
        market_zt.append(target)
        
        try:
            twitter_features = get_tweet_features(date, period)

            twitter_vt.append(twitter_features)
            twitter_zt.append(target)

            mixed_vt.append(twitter_features + market_features)
            mixed_zt.append(target)
        except NoTweetsException:
            print("No tweets found for this hour... Continuing")

    return (twitter_vt, twitter_zt, market_vt, market_zt, mixed_vt, mixed_zt)

start_date = dt.datetime(2019, 1, 1, hour=0,tzinfo=dt.timezone.utc)

# cProfile.run('get_all(start_date, data_points=3)')

# Save datapoints as a tuple
datapoints = get_all(start_date, data_points=90, daily=True)

twitter_data = (datapoints[0], datapoints[1])
market_data = (datapoints[2], datapoints[3])
mixed_data = (datapoints[4], datapoints[5])

with open('daily_twitter_datapoints.pickle', 'wb') as f:
    pickle.dump(twitter_data, f)
with open('daily_market_datapoints.pickle', 'wb') as f:
    pickle.dump(market_data, f)
with open('daily_mixed_datapoints.pickle', 'wb') as f:
    pickle.dump(mixed_data, f)

# print(get_btc_data(start_date, hours=24))
