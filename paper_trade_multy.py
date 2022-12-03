import os
import sys
import time
import pickle

import numpy as np
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
import asyncio

from binance import AsyncClient, BinanceSocketManager, Client
from binance.exceptions import BinanceAPIException
from binance.enums import *

# from nDot_crypto_db_connector import nDot_db_connector

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from tensorflow.keras.models import load_model

np.set_printoptions(threshold=5000)


class TradeOrderBook:

    def __init__(self):
        # binance
        self.async_client = None
        self.bm = None
        self.ts = None

        self.slot = {'BTCUSDT_S1': {'trade_symbol': 'BTCUSDT',
                                    'y_filter': .50,
                                    'x_type': 4,
                                    'depth': 4,
                                    'qmin': -50,
                                    'qmax': 50,
                                    'qstep': .5,
                                    'stop_delta': .01 / 100,
                                    'trailer_delta': .01 / 100,
                                    'take_delta': .3 / 100,
                                    'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
                                    'max_time_sec': 120
                                    },
                     'BTCUSDT_S2': {'trade_symbol': 'BTCUSDT',
                                    'y_filter': .75,
                                    'x_type': 4,
                                    'depth': 4,
                                    'qmin': -50,
                                    'qmax': 50,
                                    'qstep': .5,
                                    'stop_delta': .01 / 100,
                                    'trailer_delta': .01 / 100,
                                    'take_delta': .3 / 100,
                                    'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
                                    'max_time_sec': 120
                                    }
                     }

        self._traded_symbols = ['BTCUSDT']
        self._slots = ['BTCUSDT_S1', 'BTCUSDT_S2']
        self._symbol_slot = {'BTCUSDT': ['BTCUSDT_S1', 'BTCUSDT_S1']}

        self.max_depths = {'ETHUSDT': 3,
                       'BTCUSDT': 4,
                       'SOLUSDT': 3}

        self.slot_position = {'BTCUSDT_S1': {'symbol': '',
                                             'qty': 0,
                                             'income_price': 0,
                                             'stop_price': 0,
                                             'trailer_stop_price': 0,
                                             'take_price': 0,
                                             'enter_dt': None},
                              'BTCUSDT_S2': {'symbol': '',
                                             'qty': 0,
                                             'income_price': 0,
                                             'stop_price': 0,
                                             'trailer_stop_price': 0,
                                             'take_price': 0,
                                             'enter_dt': None}
                              }

        # egy adott symbolbol mennyi van
        # csak akkor követia stoplost ésatrailert ha van belőle
        self.symbol_position = self.defa_symbol_zero()

        # utolsó piaci kötésvan benne
        # ez kell az get_x_3d normálásához
        self.actual_traded_price = self.defa_symbol_array()

        # utolsó simert orderbook legjob datai
        # ez kell a buy és a stop hoz
        self.actual_bid_price = self.defa_symbol_zero()
        self.actual_ask_price = self.defa_symbol_zero()
        self.actual_bid_qty = self.defa_symbol_zero()
        self.actual_ask_qty = self.defa_symbol_zero()

        self.project_name = 'LOB'
        self.models = {}
        self.build_ai()
        self.orderbooks = self.defa_symbol_array()
        self.base_prices = self.defa_symbol_array()

        # trade monitoring
        self.monitor_stop = self.defa_slot_zero()
        self.monitor_trailer = self.defa_slot_zero()
        self.monitor_take = self.defa_slot_zero()
        self.monitor_time = self.defa_slot_zero()
        self.monitor_profit = self.defa_slot_zero()
        self.monitor_fee = 0
        self.status = {'monitor_stop': {},
                       'monitor_take': {},
                       'monitor_trailer': {},
                       'monitor_time': {},
                       'monitor_profit': {},
                       'monitor_fee': {},
                       'history_profit': []
                       }

        # status line
        self.status_counter = 0

    def buy(self, symbol, slot, qty):
        self.slot_position[slot]['symbol'] = symbol
        self.slot_position[slot]['qty'] = qty
        self.slot_position[slot]['income_price'] = self.actual_ask_price[symbol]
        self.slot_position[slot]['stop_price'] = self.slot_position[slot]['income_price'] * (1 - self.slot[slot]['stop_delta'])
        self.slot_position[slot]['trailer_stop_price'] = self.slot_position[slot]['income_price'] * (1 - self.slot[slot]['trailer_delta'])
        self.slot_position[slot]['take_price'] = self.slot_position[slot]['income_price'] * (1 + self.slot[slot]['take_delta'])
        self.slot_position[slot]['enter_dt'] = datetime.now()
        self.symbol_position[symbol] += qty
        # print("")
        # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def stop(self, symbol, slot):
        income_value = self.slot_position[slot]['qty'] * self.slot_position[slot]['income_price']
        exit_value = self.slot_position[slot]['qty'] * self.actual_bid_price[symbol]
        self.monitor_profit[slot] += (income_value - exit_value)
        self.symbol_position[symbol] -= self.slot_position[slot]['qty']
        self.slot_position[slot]['symbol'] = ''
        self.slot_position[slot]['qty'] = 0
        self.slot_position[slot]['income_price'] = 0
        self.slot_position[slot]['stop_price'] = 0
        self.slot_position[slot]['trailer_stop_price'] = 0
        self.slot_position[slot]['take_price'] = 0
        self.slot_position[slot]['enter_dt'] = None
        self.monitor_fee += round((income_value * 0.025 / 100) + (exit_value * 0.025 / 100), 2)

    def get_status(self):

        self.status = {'monitor_stop': self.monitor_stop,
        self.status                      'monitor_take': self.monitor_take,
        self.status               'monitor_trailer': self.monitor_trailer,
        self.status               'monitor_time': self.monitor_time,
        self.status               'monitor_profit': self.monitor_profit,
        self.status               'monitor_fee': self.monitor_fee,
                       }

        if len(self.status['history_profit']) == 0:
            self.status['history_profit'].append(self.monitor_profit)
        elif self.status['history_profit'][-1] != self.monitor_profit:
            self.status['history_profit'].append(self.monitor_profit)

        return self.status

    def open_pnl(self):
        pnl = 0
        for slot in self.slot_position:
            if self.slot_position[slot]['qty'] > 0:
                income_value = self.slot_position[slot]['qty'] * self.slot_position[slot]['income_price']
                exit_value = self.slot_position[slot]['qty'] * self.actual_bid_price[self.slot_position[slot]['symbol']]
                pnl += round(income_value - exit_value, 2)
        return pnl
    
    def slot_in_position(self, slot):
        if self.slot_position[slot]['qty'] > 0:
            return True
        elif self.slot_position[slot]['qty'] == 0:
            return False
        else:
            print("Minusz pozíció!!!!!")
            # TODO ezt a hibát kezelni kell
            sys.exit()

    def defa_symbol_zero(self):
        ret = {}
        for sy in self._traded_symbols:
            ret[sy] = 0.0
        return ret

    def defa_slot_zero(self):
        ret = {}
        for sy in self._slots:
            ret[sy] = 0.0
        return ret

    def defa_symbol_array(self):
        ret = {}
        for sy in self._traded_symbols:
            ret[sy] = []
        return ret

    def build_ai(self):
        for sl in self.slot:
            tf_model_name = self.slot[sl]['tf_model_file']
            self.models[sl] = load_model(tf_model_name)

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
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "trade"))
            i_socket_list.append(self.get_socket_name(symbol, "depth20"))
            i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        async with self.ts as tscm:
            # dot = '.'
            while True:
                self.status_counter += 1
                if self.status_counter > 100:
                    pickle.dump(self.get_status(), open("paper_trade_multy_status.pickle", "wb"))
                    print('save')
                    self.status_counter = 0

                    # profit_rounded = {key: round(self.monitor_profit[key], 2) for key in self.monitor_profit}
                    # pl = f"Stop: {self.monitor_stop}"\
                    #      f" take: {self.monitor_take} " \
                    #      f" trailer: {self.monitor_trailer}" \
                    #      f" last: {self.monitor_time}" \
                    #      f" total profit: {profit_rounded} / {self.monitor_fee} " \
                    #      f"open_pnl: {self.open_pnl()}"
                    # pl = pl.replace('{', '').replace('}', '').replace("'", '')
                    # print("\r" + pl, end="")

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
                    if len(self.actual_traded_price[symbol]) >= 3:
                        calc_base_price = (self.actual_traded_price[symbol][-1] + self.actual_traded_price[symbol][-2] + self.actual_traded_price[symbol][-3]) / 3
                    else:
                        calc_base_price = self.actual_traded_price[symbol][-1]
                    self.base_prices[symbol].append(calc_base_price)
                    if len(self.base_prices[symbol]) > self.max_depths[symbol]:
                        self.base_prices[symbol] = self.base_prices[symbol][1:]

                    self.orderbooks[symbol].append(res['data'])
                    if len(self.orderbooks[symbol]) > self.max_depths[symbol]:
                        self.orderbooks[symbol] = self.orderbooks[symbol][1:]
                        for slot in self._symbol_slot[symbol]:
                            await self.trader(symbol, slot, self.orderbooks[symbol], self.base_prices[symbol])

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

                    self.actual_traded_price[symbol].append(float(res['data']['p']))
                    if len(self.actual_traded_price) > 3:
                        self.actual_traded_price = self.actual_traded_price[1:]

                elif stype == "bookTicker":

                    # {
                    #   "u":400900217,     // order book updateId
                    #   "s":"BNBUSDT",     // symbol
                    #   "b":"25.35190000", // best bid price
                    #   "B":"31.21000000", // best bid qty
                    #   "a":"25.36520000", // best ask price
                    #   "A":"40.66000000"  // best ask qty
                    # }

                    self.actual_bid_price[symbol] = float(res['data']['b'])
                    self.actual_bid_qty[symbol] = float(res['data']['B'])

                    self.actual_ask_price[symbol] = float(res['data']['a'])
                    self.actual_ask_qty[symbol] = float(res['data']['A'])
                    
                    if self.symbol_position[symbol] > 0:
                        for slot in self._symbol_slot[symbol]:
                            act_traile_price = self.actual_bid_price[symbol] * (1 - self.slot[slot]['trailer_delta'])
                            self.slot_position[slot]['trailer_stop_price'] = max(self.slot_position[slot]['trailer_stop_price'], act_traile_price)
                            # print(self.slot_position)
                            if self.actual_bid_price[symbol] >= self.slot_position[slot]['take_price']:
                                self.stop(symbol, slot)
                                self.monitor_take[slot] += 1
                            elif self.slot_position[slot]['enter_dt'] + timedelta(seconds=self.slot[slot]['max_time_sec']) < datetime.now():
                                self.stop(symbol, slot)
                                self.monitor_time[slot] += 1
                            elif self.actual_bid_price[symbol] < self.slot_position[slot]['stop_price']:
                                self.stop(symbol, slot)
                                self.monitor_stop[slot] += 1
                            elif self.actual_bid_price[symbol] < self.slot_position[slot]['trailer_stop_price']:
                                self.stop(symbol, slot)
                                self.monitor_trailer[slot] += 1

    # async def show_prices(self):
    #
    #     future_prices = np.array(self.prices_future)
    #     fp = future_prices[0]
    #     future_prices = np.divide(future_prices, fp)
    #
    #     fig, ax = plt.subplots()
    #     fig.set_size_inches(14, 6)
    #     ax.plot(future_prices)
    #
    #     ax.set(xlabel='time (s)', ylabel='price', title=self.title)
    #     ax.grid()
    #     plt.tight_layout()
    #     plt.show()

    async def trader(self, symbol, slot, orderbooks, base_prices):
        x_type = self.slot[slot]['x_type']
        qmin = self.slot[slot]['qmin']
        qmax = self.slot[slot]['qmax']
        qstep = self.slot[slot]['qstep']
        depth = self.slot[slot]['depth']
        px = self.get_x_3d(orderbooks, base_prices, x_type=x_type, qmin=qmin, qmax=qmax, qstep=qstep, depth=depth)

        if len(px) > 0 and not np.any(np.isnan(px)) and not np.any(np.isinf(px)):

            # aget_x_3d 3 dimenziós tömböt ad vissza, de a neur 4 dimenzóat vár.
            # kibővítem 1 deimenzióval
            x = np.zeros((1, px.shape[0], px.shape[1], px.shape[2]), dtype=np.float32)
            x[0] = px

            y_result = self.models[slot].predict(x, verbose=0, batch_size=1)
            y_predict = np.argmax(y_result, axis=1)
            
            y_filter = self.slot[slot]['y_filter']
            
            if y_predict[0] == 1 and y_result[0][1] > y_filter and not self.slot_in_position(slot):
                self.buy(symbol, slot, 1)

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
    n_tob = TradeOrderBook()

    n_tob.start_stream()

    while True:
        pass
    