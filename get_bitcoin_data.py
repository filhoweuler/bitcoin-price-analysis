import cryptocompyre as cpyre
from influxdb import InfluxDBClient

db_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'bitcoin_tweets')

def bitcoin_price_to_json(data):
    d = {
        'measurement': 'bitcoin',
        'tags': {

        },
        'time': data['time'],
        'fields': {
            'open': float(data['open']),
            'close': float(data['close']),
            'high': float(data['high']),
            'low': float(data['low']),
            'volumeto': float(data['volumeto'])
        }
    }
    return [d]

client = cpyre.CryptoComPyre('f875c451c6334add2227564d33b737c9ef5537da145d841204847758edf75bd4')

for data in client.get_hourly_data(limit=300, tots=1546887600 ):
    print(data)
    db_client.write_points(bitcoin_price_to_json(data))