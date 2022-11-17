import time

from binance.client import Client
import datetime


def unix_to_datetime(ts):
    ts = int(ts)
    return datetime.datetime.fromtimestamp(int(ts) / 1000)


api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
client = Client(api_key, api_secret)

for ti in range(10):
    pr = []
    for i in range(60 * 3):
        trades = client.get_historical_trades(symbol="BTCUSDT", limit=1000)
        for tr in trades:
            pr.append(float(tr['price']))

        maxp = max(pr)
        minp = min(pr)

        print(i, maxp, minp, round(maxp - minp,2), round(maxp * ((0.075/100) * 2),2))

        # r_o = client.get_order_book(symbol='BTCUSDT')
        # print(((float(r_o['bids'][0][0]) + float(r_o['asks'][0][0])) / 2) - float(trades[-1]['price']))
        time.sleep(1)





