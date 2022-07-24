from binance.client import Client
import datetime

def unix_to_datetime(ts):
    ts = int(ts)
    return datetime.datetime.fromtimestamp(int(ts) / 1000)


api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
client = Client(api_key, api_secret)

trades = client.get_historical_trades(symbol="NMRUSDT")
print(unix_to_datetime(trades[-1]['time']))
print("date_time", datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"))





