'''
Lets start importing a 3 month period ...
01/01/2019 - 30/03/2019
'''

import twitterscraper
import pickle
import time
import datetime as dt
import logging

start_date = dt.date(2019, 1, 1)

for date in (start_date + dt.timedelta(n) for n in range(90)):

    start_time = time.time()

    print(f"Starting on day {date} ...")
    tweets = twitterscraper.query_tweets("bitcoin OR Bitcoin OR btc OR BTC OR Btc", begindate=date, enddate=date + dt.timedelta(1), lang='en')
    
    logging.getLogger('twitterscraper').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    logging.getLogger('requests').setLevel(logging.CRITICAL)

    print(f"---- Found {len(tweets)} tweets. ----")
    print(f"---- Took {time.time() - start_time} seconds ----")

    file_name = 'tweets/btc_tweets_' + str(date)

    with open(file_name, 'wb') as f:
        pickle.dump(tweets, f)

    

