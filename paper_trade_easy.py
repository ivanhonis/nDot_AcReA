import os
import sys
import time

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import asyncio

from binance import AsyncClient, BinanceSocketManager, Client
from binance.exceptions import BinanceAPIException
from binance.enums import *

from nDot_crypto_db_connector import nDot_db_connector

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from tensorflow.keras.models import load_model

np.set_printoptions(threshold=5000)



class TradeOrderBook:

    def __init__(self):
        # self.traded_symbols = ['ETHUSDT', 'BTCUSDT', 'SOLUSDT']
        self.traded_symbols = ['BTCUSDT']

        self.slot_sttings = {'BTCUSDT1': {'trade_symbol': 'BTCUSDT',
                                 'filter': .70,
                                 'x_type': 4,
                                 'depth': 4
                                 }

                     }

        self.slots = ['BTCUSDT1']

        self.traded_symbols = ['BTCUSDT']
        self.filters = {'ETHUSDT': .89,
                        'BTCUSDT': .70,
                        'SOLUSDT': .69}

        self.depths = {'ETHUSDT': 3,
                       'BTCUSDT': 4,
                       'SOLUSDT': 3}

        self.x_types = {'ETHUSDT': 3,
                       'BTCUSDT': 4,
                       'SOLUSDT': 3}

        self.actual_price = self.defa_symbol_array()
        self.depth = 3
        self.project_name = 'LOB'
        self.models = {}
        self.build_ai()
        self.orderbooks = self.defa_symbol_array()
        self.base_prices = self.defa_symbol_array()
        self.prices_future = self.defa_symbol_array()

        #  trading
        self.trade_flag = False
        self.trade_symbol = ""
        self.trade_start_dt = datetime.now()
        self.title = ""


        # monitoring
        self.trade_counter = self.defa_symbol_zero()
        self.monitor_counter = 0
        self.monitor_stop = self.defa_symbol_zero()
        self.monitor_take = self.defa_symbol_zero()
        self.monitor_last = self.defa_symbol_zero()
        self.monitor_profit = self.defa_symbol_zero()

    def defa_symbol_zero(self):
        ret = {}
        for sy in self.traded_symbols:
            ret[sy] = 0.0
        return ret

    def defa_symbol_array(self):
        ret = {}
        for sy in self.traded_symbols:
            ret[sy] = []
        return ret

    def build_ai(self):
        for sy in self.traded_symbols:
            tf_model_name = f"projects/LOB/{sy}/nDot_TF_MODEL_{self.project_name}_{sy}.h5"
            # print(tf_model_name)
            self.models[sy] = load_model(tf_model_name)

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

    async def asyc_websocket(self):
        i_socket_list = []
        for symbol in self.traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "trade"))
            i_socket_list.append(self.get_socket_name(symbol, "depth20"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        orderbooks = []
        base_prices = []

        async with self.ts as tscm:
            # dot = '.'
            while True:
                self.monitor_counter += 1
                if self.monitor_counter > 100:
                    self.monitor_counter = 0
                    profit_rounded = {key: round(self.monitor_profit[key], 2) for key in self.monitor_profit}
                    pl = f"Trades: {self.trade_counter} stop: {self.monitor_stop}"\
                         f" take: {self.monitor_take} last: {self.monitor_last}"\
                         f" profit: {profit_rounded}"
                    pl = pl.replace('{', '').replace('}', '').replace("'", '')

                    print("\r" + pl, end="")


                res = await tscm.recv()
                # print(res)
                symbol = res['stream'].split('@')[0].upper()
                stype = res['stream'].split('@')[1]
                if stype == "depth20":
                    # {
                    #   "lastUpdateId": 160,  // Last update ID
                    #   "bids": [             // Bids to be updated
                    #     [
                    #       "0.0024",         // Price level to be updated
                    #       "10"              // Quantity
                    #     ]
                    #   ],
                    #   "asks": [             // Asks to be updated
                    #     [
                    #       "0.0026",         // Price level to be updated
                    #       "100"             // Quantity
                    #     ]
                    #   ]
                    # }
                    if len(self.actual_price[symbol]) >= 3:
                        calc_base_price = (self.actual_price[symbol][-1] + self.actual_price[symbol][-2] + self.actual_price[symbol][-3]) / 3
                    else:
                        calc_base_price = self.actual_price[symbol][-1]
                    self.base_prices[symbol].append(calc_base_price)
                    if len(self.base_prices[symbol]) > self.depths[symbol]:
                        self.base_prices[symbol] = self.base_prices[symbol][1:]

                    self.orderbooks[symbol].append(res['data'])
                    if len(self.orderbooks[symbol]) > self.depths[symbol]:
                        self.orderbooks[symbol] = self.orderbooks[symbol][1:]
                        if not self.trade_flag:
                            await self.trader(symbol, self.orderbooks[symbol], self.base_prices[symbol])

                elif stype == "trade":

                    # {
                    #   "e": "trade",     // Event type
                    #   "E": 123456789,   // Event time
                    #   "s": "BNBBTC",    // Symbol
                    #   "t": 12345,       // Trade ID
                    #   "p": "0.001",     // Price
                    #   "q": "100",       // Quantity
                    #   "b": 88,          // Buyer order ID
                    #   "a": 50,          // Seller order ID
                    #   "T": 123456785,   // Trade time
                    #   "m": true,        // Is the buyer the market maker?
                    #   "M": true         // Ignore
                    # }

                    self.actual_price[symbol].append(float(res['data']['p']))
                    if len(self.actual_price) > 3:
                        self.actual_price = self.actual_price[1:]
                    if self.trade_flag:
                        self.prices_future[symbol].append(self.actual_price[symbol][-1])
                        if self.trade_start_dt + timedelta(seconds=5) < datetime.now():
                            act_profit = 0
                            for pf in self.prices_future[self.trade_symbol][5:-1]:
                                if ((pf / self.prices_future[self.trade_symbol][0]) - 1) < -0.065 / 100:
                                    act_profit = pf - self.prices_future[self.trade_symbol][0]
                                    self.monitor_stop[symbol] += 1
                                    # print("stop:", pf, self.prices_future[self.trade_symbol][0])
                                    break
                                elif ((pf / self.prices_future[self.trade_symbol][0]) - 1) >= 0.02 / 100:
                                    act_profit = pf - self.prices_future[self.trade_symbol][0]
                                    self.monitor_take[symbol] += 1
                                    # print("take:", pf, self.prices_future[self.trade_symbol][0])
                                    break
                            if act_profit == 0:
                                act_profit = self.prices_future[self.trade_symbol][-1] - self.prices_future[self.trade_symbol][0]
                                self.monitor_last[symbol] += 1
                                # print("last:", self.prices_future[self.trade_symbol][-1], self.prices_future[self.trade_symbol][0])

                            # p0 = self.prices_future[self.trade_symbol][0]
                            # pmax = max(self.prices_future[self.trade_symbol])
                            self.monitor_profit[symbol] += act_profit
                            # print(symbol, p0, pmax, self.monitor_profit)
                            # await self.show_prices()
                            self.trade_flag = False

    async def show_prices(self):

        future_prices = np.array(self.prices_future)
        fp = future_prices[0]
        future_prices = np.divide(future_prices, fp)

        fig, ax = plt.subplots()
        fig.set_size_inches(14, 6)
        ax.plot(future_prices)

        ax.set(xlabel='time (s)', ylabel='price', title=self.title)
        ax.grid()
        plt.tight_layout()
        plt.show()

    async def trader(self, symbol, orderbooks, base_prices):
        px = self.get_x_3d(orderbooks, base_prices, x_type=self.x_types[symbol], qmin=-50, qmax=50, qstep=.5, depth=self.depths[symbol])

        if len(px) > 0 and not np.any(np.isnan(px)) and not np.any(np.isinf(px)):
            x = np.zeros((1, px.shape[0], px.shape[1], px.shape[2]), dtype=np.float32)
            x[0] = px
            # print(x.shape)
            # print(np.sum(x))
            y_result = self.models[symbol].predict(x, verbose=0, batch_size=1)
            # print(y_result)
            y_predict = np.argmax(y_result, axis=1)
            # print(y_result, y_predict)
            # y_result[1]
            if y_predict[0] == 1 and y_result[0][1] > self.filters[symbol]:
                self.trade_counter[symbol] += 1
                # print("GOGOGO", symbol, self.trade_counter)

                self.trade_start_dt = datetime.now()
                self.trade_symbol = symbol
                self.prices_future[symbol] = []
                self.trade_flag = True
                self.title = "1111111111"

            # else:
            #     self.show = True
            #     self.title = "0000000000000"
            #     self.prices_future = []

    def start_stream(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket())
        loop.close()

    def get_x_3d(self, orderbooks, base_prices, x_type=3, qmin=-50, qmax=50, qstep=.5, depth=3):
        orderbooks = orderbooks[::-1]
        base_prices = base_prices[::-1]
        # print(orderbooks)
        # print(base_prices)

        # base_prices = []
        # orderbooks = []

        if x_type == 3:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[d], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / np.sum(bids_scaled_qt)
                asks_scaled_qt = asks_scaled_qt / np.sum(asks_scaled_qt)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 4:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[0], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / np.sum(bids_scaled_qt)
                asks_scaled_qt = asks_scaled_qt / np.sum(asks_scaled_qt)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])


if __name__ == '__main__':
    n_cdc = nDot_db_connector()
    n_tob = TradeOrderBook()

    n_tob.start_stream()

    while True:
        pass

    # project_name = 'LOB'
    # sufix = ""
    # n_tf_model_name = sufix + 'nDot_TF_MODEL_' + project_name + ".h5"
    # model = load_model(n_tf_model_name)
    # symbol = "BTCUSDT"
    #
    # depth = 3
    #
    # maxit = 60 * 60 * 24 * 2
    # count_1 = 0
    #
    # date_time_string_ob = "2022-11-01 00:00:00"
    # dt = datetime.fromisoformat(date_time_string_ob)
    #
    # show = False
    # title = ""
    #
    # for ix in range(maxit):
    #     print("\r" + f"job ready: {ix} / {maxit}  -  {count_1}", end="")
    #
    #     base_prices = []
    #     orderbooks = []
    #
    #     for d in range(depth):
    #
    #         base_dt_tick = dt - timedelta(seconds=d + 1)
    #         res_tick_base = n_cdc.get_tick_data(symbol, base_dt_tick)
    #
    #         if res_tick_base and res_tick_base['last']:
    #             base_prices.append(float(res_tick_base['last']['price']))
    #
    #             base_dt_ob = dt - timedelta(seconds=d)
    #             res_ob = n_cdc.get_orderbook_data(symbol, base_dt_ob)
    #             if res_ob:
    #                 orderbooks.append(res_ob)
    #
    #     if len(orderbooks) == 3:
    #
    #         px = n_tob.get_x_3d(orderbooks, base_prices, x_type=3, qmin=-50, qmax=50, qstep=.5, depth=3)
    #
    #         if len(px) > 0 and not np.any(np.isnan(px)) and not np.any(np.isinf(px)):
    #             x = np.zeros((1, px.shape[0], px.shape[1], px.shape[2]), dtype=np.float32)
    #             x[0] = px
    #             # print(x.shape)
    #             # print(np.sum(x))
    #             y_result = model.predict(x, verbose=0, batch_size=1)
    #             print(y_result)
    #             y_predict = np.argmax(y_result, axis=1)
    #             # print(y_predict)
    #             # y_result[1]
    #             if y_predict[0] == 1 and y_result[0][1] > .7589:
    #                 # print(y_predict[0])
    #                 count_1 += 1
    #                 show = True
    #                 title = "1111111"
    #             else:
    #                 if ix % 1000 == 0:
    #                     show = True
    #                     title = "000000"
    #
    #             if show:
    #                 show = False
    #
    #                 future_prices_res = []
    #
    #                 base_dt_tick0 = dt
    #                 future_prices_res.append(n_cdc.get_tick_data(symbol, base_dt_tick0))
    #
    #                 base_dt_tick1 = dt + timedelta(seconds=1)
    #                 future_prices_res.append(n_cdc.get_tick_data(symbol, base_dt_tick1))
    #
    #                 base_dt_tick2 = dt + timedelta(seconds=2)
    #                 future_prices_res.append(n_cdc.get_tick_data(symbol, base_dt_tick2))
    #
    #                 future_prices = []
    #
    #                 for fr in future_prices_res:
    #                     for p in fr['all']:
    #                         future_prices.append(float(p['price']))
    #
    #                 future_prices = np.array(future_prices)
    #                 fp = future_prices[0]
    #                 future_prices = np.divide(future_prices, fp)
    #
    #                 fig, ax = plt.subplots()
    #                 ax.plot(future_prices)
    #
    #                 ax.set(xlabel='time (s)', ylabel='price', title=title)
    #                 ax.grid()
    #
    #                 fig.savefig("test.png")
    #                 plt.show()
    #
    #     dt = dt + timedelta(seconds=1)
