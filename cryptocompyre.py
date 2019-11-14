import requests
import time

class CryptoComPyre:
    def __init__(self, api_key):
        self.api_key = api_key
        self.BASE_URL = 'https://min-api.cryptocompare.com/data/v2'

    def __get_data(self, response):
        return response.json()['Data']['Data']

    def get_hourly_data(self, fsym='BTC', tsym='USD', aggregate=1, limit=10, tots=int(time.time())):
        response = requests.get(f'{self.BASE_URL}/histohour?fsym={fsym}&tsym={tsym}&aggregate={aggregate}&limit={limit}&toTs={tots}&api_key={self.api_key}')
        return self.__get_data(response)