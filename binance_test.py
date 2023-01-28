import sys
import time
import os
import psutil

import pickle
from datetime import datetime, timedelta

from numba import int32, float32
from numba.experimental import jitclass
from numba import njit, objmode, void
import numba
# import psutil

import asyncio
from threading import Thread
from multiprocessing import cpu_count, Process, Array

import numpy as np
from scipy.signal import savgol_filter

# from sklearn import linear_model
# from sklearn.preprocessing import PolynomialFeatures
# from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import LinearRegression


import datetime
from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
from binance.exceptions import BinanceAPIException


class BTest:

    def __init__(self):

        self.symbol = "BTCBUSD"

        self.async_client = None
        self.bm = None
        self.ts = None

        self.api_key = "sNtEg0vnKFm09xKf8v9VJWIYspFFouJN5vJO9bSgUSAU8eoAUa5OaMuJYLLsuswr"
        self.api_secret = "Hhu6MPTOdEKBjZyooPr1JxbgiLME3VFJdymqJrtytrMywKatP08Y5G1Sb9ZuJv4S"
        self.bx_client = Client(self.api_key, self.api_secret)

        self.count = 0
        self.get_info()
        self.buy()
        self.sell()
        # self.start_threads()

    def start_threads(self):
        task4 = Thread(target=self.bookticker_detect, args=[])
        # task5 = Thread(target=self.deal_hunter_thr, args=[])
        # task6 = Thread(target=self.data_transfer, args=[])
        task4.start()
        task4.join()

    def bookticker_detect(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_websocket_bookticker_detect())
        loop.close()

    def get_info(self):
        info = self.bx_client.get_isolated_margin_symbol(symbol='BTCBUSD')
        print(info)
        info = self.bx_client.get_margin_price_index(symbol='BTCBUSD')
        print(info)

    def buy(self):
        symbol = "BTCBUSD"
        qty = 0.0006
        dt1 = datetime.datetime.now()
        # order1 = self.bx_client.order_market(symbol=symbol,
        #                                      side=SIDE_BUY,
        #                                      quantity=str(qty))

        order1 = self.bx_client.create_margin_order(symbol=symbol,
                                                    side=SIDE_BUY,
                                                    type=ORDER_TYPE_MARKET,
                                                    quantity=str(qty),
                                                    isIsolated='TRUE')

        print('speed', datetime.datetime.now() - dt1)
        print(order1)

    def sell(self):
        symbol = "BTCBUSD"
        qty = 0.0012
        dt1 = datetime.datetime.now()
        # order2 = self.bx_client.order_market(symbol=symbol,
        #                                      side=SIDE_SELL,
        #                                      quantity=str(qty))

        order2 = self.bx_client.create_margin_order(symbol=symbol,
                                                    side=SIDE_SELL,
                                                    type=ORDER_TYPE_MARKET,
                                                    quantity=str(qty),
                                                    isIsolated='TRUE')

        print('speed:', datetime.datetime.now() - dt1)
        print(order2)


    @staticmethod
    def get_socket_name(symbol, i_type):
        if i_type == "kline1":
            return symbol.lower() + '@kline_1m'
        elif i_type == "bookticker":
            return symbol.lower() + '@bookTicker'
        elif i_type == "depth20":
            return symbol.lower() + '@depth20'
        elif i_type == "ticker":
            return symbol.lower() + '@ticker'
        elif i_type == "trade":
            return symbol.lower() + '@trade'

    def forced_exit(self):
        print('exit')
        current_system_pid = os.getpid()
        process = psutil.Process(current_system_pid)
        process.terminate()

    async def async_websocket_bookticker_detect(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        # r1 = -10
        # r2 = -40

        async with self.ts as tscm:
            while True:
                res = await tscm.recv()
                # ntr_nb.add_data(float(res['data']['b']), float(res['data']['a']))
                # print(res)
                # symbol = res['stream'].split('@')[0].upper()
                # stype = res['stream'].split('@')[1]

                # {
                #   "u":400900217,     // order book updateId
                #   "s":"BNBUSDT",     // symbol
                #   "b":"25.35190000", // best bid price
                #   "B":"31.21000000", // best bid qty
                #   "a":"25.36520000", // best ask price
                #   "A":"40.66000000"  // best ask qty
                # }
                # Detect
                bid = float(res['data']['b'])
                # bid_qty = float(res['data']['B'])
                ask = float(res['data']['a'])
                # ask_qty = float(res['data']['A'])
                if self.count == 500:
                    print('BUY ask:', ask, 'bid:', bid)
                    self.buy()
                    self.forced_exit()
                # elif self.count == 1000:
                #     print('SELL ask:', ask, 'bid:', bid)
                #     self.sell()
                #     sys.exit()

                self.count += 1



if __name__ == '__main__':
    n_btest = BTest()
    # while True:
    #     time.sleep(100)
