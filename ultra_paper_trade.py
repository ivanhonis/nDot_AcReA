import os
import random
import sys
import time
import pickle

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
import asyncio
from threading import Thread

from binance import AsyncClient, BinanceSocketManager, Client
from binance.exceptions import BinanceAPIException
from binance.enums import *

# from nDot_crypto_db_connector import nDot_db_connector

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from tensorflow.keras.models import load_model

import tensorflow as tf

np.set_printoptions(threshold=5000)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', '{:8,.4f}'.format)
pd.set_option('display.max_colwidth', None)


class LiteModel:

    @classmethod
    def from_file(cls, model_path):
        return LiteModel(tf.lite.Interpreter(model_path=model_path))

    @classmethod
    def from_keras_model(cls, kmodel):
        converter = tf.lite.TFLiteConverter.from_keras_model(kmodel)
        tflite_model = converter.convert()
        return LiteModel(tf.lite.Interpreter(model_content=tflite_model))

    def __init__(self, interpreter):
        self.interpreter = interpreter
        self.interpreter.allocate_tensors()
        input_det = self.interpreter.get_input_details()[0]
        output_det = self.interpreter.get_output_details()[0]
        self.input_index = input_det["index"]
        self.output_index = output_det["index"]
        self.input_shape = input_det["shape"]
        self.output_shape = output_det["shape"]
        self.input_dtype = input_det["dtype"]
        self.output_dtype = output_det["dtype"]

    def predict(self, inp):
        inp = inp.astype(self.input_dtype)
        count = inp.shape[0]
        out = np.zeros((count, self.output_shape[1]), dtype=self.output_dtype)
        for i in range(count):
            self.interpreter.set_tensor(self.input_index, inp[i:i + 1])
            self.interpreter.invoke()
            out[i] = self.interpreter.get_tensor(self.output_index)[0]
        return out

    def predict_single(self, inp):
        """ Like predict(), but only for a single record. The input data can be a Python list. """
        inp = np.array([inp], dtype=self.input_dtype)
        self.interpreter.set_tensor(self.input_index, inp)
        self.interpreter.invoke()
        out = self.interpreter.get_tensor(self.output_index)
        return out[0]


class TradeOrderBook:

    def __init__(self):
        # binance
        self.async_client = None
        self.bm = None
        self.ts = None

        self.slot = {
            # 'BTCUSDT_SX3': {'trade_symbol': 'BTCUSDT',
            #                 'y_filter': .60,
            #                 'x_type': 5,
            #                 'depth': 3,
            #                 'qmin': -50,
            #                 'qmax': 50,
            #                 'qstep': .5,
            #                 'stop_delta': .3 / 100,
            #                 'trailer_delta': .0012 / 100,
            #                 'take_delta': .9 / 100,
            #                 'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
            #                 'max_time_sec': 120,
            #                 'extra_time_sec': 120,
            #                 'extra_stop_delta': .0075 / 100,
            #                 'extra_trailer_delta': .0006 / 100,
            #                 'extra_take_delta': .9 / 100,
            #                 }
            'BTCUSDT_SX3': {'trade_symbol': 'BTCUSDT',
                            'y_filter': .65,
                            'x_type': 5,
                            'depth': 3,
                            'qmin': -50,
                            'qmax': 50,
                            'qstep': .5,
                            'stop_delta': .1 / 100,
                            'trailer_delta': .2 / 100,
                            'take_delta': .2 / 100,
                            'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
                            'max_time_sec': 60 * 5,
                            'extra_time_sec': 0,
                            'extra_stop_delta': .0075 / 100,
                            'extra_trailer_delta': .0006 / 100,
                            'extra_take_delta': .9 / 100,
                            }
        }

        self._traded_symbols = []
        self._slots = []
        self._symbol_slot = {}
        self._slot_position = {}
        self._defa()

        # egy adott symbolbol mennyi van
        # csak akkor követia stop lost és a trailert ha van belőle
        # ez pozíció vezetésre nem szolgál csak technikai gyorsításra, hogy ne kellejen kalkulálni
        self.total_symbol_position = self.defa_symbol_zero()

        self.max_depths = {'ETHUSDT': 3,
                       'BTCUSDT': 4,
                       'SOLUSDT': 3}

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
        self.monitor_trailer_avg_time = self.defa_slot_zero()
        self.monitor_take = self.defa_slot_zero()
        self.monitor_time = self.defa_slot_zero()
        self.monitor_profit = self.defa_slot_zero()
        self.monitor_max_loss = self.defa_slot_zero()
        self.monitor_fee = self.defa_slot_zero()
        self.monitor_trailer_deal_times = []
        self.status = {'monitor_stop': {},
                       'monitor_take': {},
                       'monitor_trailer': {},
                       'monitor_trailer_avg_time': {},
                       'monitor_time': {},
                       'monitor_profit': {},
                       'monitor_max_loss': {},
                       'monitor_fee': {},
                       }

    def strat_threads(self):
        task1 = Thread(target=self.bookticker_thr, args=[])
        # task2 = Thread(target=self.trade_thr, args=[])
        task3 = Thread(target=self.orderbook_thr, args=[])

        task1.start()
        # time.sleep(.5)
        # task2.start()
        time.sleep(5)
        task3.start()

    def orderbook_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_orderbook())
        loop.close()

    def bookticker_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_bookticker())
        loop.close()

    def trade_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_trade())
        loop.close()

    def _defa(self):
        self._slots = []
        for sl in self.slot:
            self._slots.append(sl)

        self._traded_symbols = []
        for sl in self.slot:
            if self.slot[sl]['trade_symbol'] not in self._traded_symbols:
                self._traded_symbols.append(self.slot[sl]['trade_symbol'])

        self._symbol_slot = {}
        for symbol in self._traded_symbols:
            self._symbol_slot = {symbol: []}
            
            for sl in self.slot:
                if self.slot[sl]['trade_symbol'] == symbol:
                    self._symbol_slot[symbol].append(sl)

        self._slot_position = {}
        for sl in self.slot:
            self._slot_position[sl] = {'symbol': '',
                                       'qty': 0,
                                       'income_price': 0,
                                       'stop_price': 0,
                                       'trailer_stop_price': 0,
                                       'trailer_minimum_price': 0,
                                       'take_price': 0,
                                       'enter_dt': None,
                                       'exit_dt': None,
                                       'extra_dt': None,
                                       'extra_flag': False
                                       }

    def buy(self, symbol, slot, qty):
        self._slot_position[slot]['symbol'] = symbol
        self._slot_position[slot]['qty'] = qty
        self._slot_position[slot]['income_price'] = self.actual_ask_price[symbol]
        self._slot_position[slot]['stop_price'] = self._slot_position[slot]['income_price'] * (1 - self.slot[slot]['stop_delta'])
        self._slot_position[slot]['trailer_stop_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['trailer_delta'])
        self._slot_position[slot]['trailer_minimum_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['trailer_delta'])
        self._slot_position[slot]['take_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['take_delta'])
        self._slot_position[slot]['enter_dt'] = datetime.now()
        self._slot_position[slot]['exit_dt'] = self._slot_position[slot]['enter_dt'] + timedelta(seconds=self.slot[slot]['max_time_sec'])
        self._slot_position[slot]['extra_dt'] = self._slot_position[slot]['exit_dt'] + timedelta(seconds=self.slot[slot]['extra_time_sec'])
        self._slot_position[slot]['extra_flag'] = False
        self.total_symbol_position[symbol] += qty
        # print("")
        # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def reset_loss_reducer(self, symbol, slot):
        # self._slot_position[slot]['symbol'] = symbol
        # self._slot_position[slot]['qty'] = qty
        # self._slot_position[slot]['income_price'] = self.actual_ask_price[symbol]
        actual_bid = self.actual_bid_price[symbol]
        self._slot_position[slot]['stop_price'] = actual_bid * (1 - self.slot[slot]['extra_stop_delta'])
        self._slot_position[slot]['trailer_stop_price'] = actual_bid * (1 + self.slot[slot]['extra_trailer_delta'])
        self._slot_position[slot]['trailer_minimum_price'] = actual_bid * (1 + self.slot[slot]['extra_trailer_delta'])
        self._slot_position[slot]['take_price'] = self._slot_position[slot]['income_price']
        # self._slot_position[slot]['enter_dt'] = datetime.now()
        # self._symbol_position[symbol] += qty
        # print("")
        # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def reset_extra_profit(self, symbol, slot):
        # self._slot_position[slot]['symbol'] = symbol
        # self._slot_position[slot]['qty'] = qty
        # self._slot_position[slot]['income_price'] = self.actual_ask_price[symbol]
        actual_bid = self.actual_bid_price[symbol]
        self._slot_position[slot]['stop_price'] = self._slot_position[slot]['income_price']
        self._slot_position[slot]['trailer_stop_price'] = .9
        self._slot_position[slot]['trailer_minimum_price'] = actual_bid * (1 + self.slot[slot]['extra_trailer_delta'])
        self._slot_position[slot]['take_price'] = self._slot_position[slot]['income_price']
        # self._slot_position[slot]['enter_dt'] = datetime.now()
        # self._symbol_position[symbol] += qty
        # print("")
        # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def stop(self, symbol, slot):
        income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
        exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
        # print(income_value, exit_value)
        self.monitor_profit[slot] += (exit_value - income_value)
        print("Close profit:", (exit_value - income_value))
        self.total_symbol_position[symbol] -= self._slot_position[slot]['qty']
        self._slot_position[slot]['symbol'] = ''
        self._slot_position[slot]['qty'] = 0
        self._slot_position[slot]['income_price'] = 0
        self._slot_position[slot]['stop_price'] = 0
        self._slot_position[slot]['trailer_stop_price'] = 0
        self._slot_position[slot]['trailer_minimum_price'] = 0
        self._slot_position[slot]['take_price'] = 0
        self._slot_position[slot]['enter_dt'] = None
        self._slot_position[slot]['exit_dt'] = None
        self._slot_position[slot]['extra_dt'] = None
        self._slot_position[slot]['extra_flag'] = False
        self.monitor_fee[slot] += round((income_value * 0.025 / 100) + (exit_value * 0.025 / 100), 2)

    def get_status(self):

        self.status['monitor_stop'] = self.monitor_stop
        self.status['monitor_take'] = self.monitor_take
        self.status['monitor_trailer'] = self.monitor_trailer
        self.status['monitor_trailer_avg_time'] = self.monitor_trailer_avg_time
        self.status['monitor_time'] = self.monitor_time
        self.status['monitor_profit'] = self.monitor_profit
        self.status['monitor_max_loss'] = self.monitor_max_loss
        self.status['monitor_fee'] = self.monitor_fee


        # if len(self.status['history_profit']) == 0:
        #     self.status['history_profit'].append(self.monitor_profit)
        # elif self.status['history_profit'][-1] != self.monitor_profit:
        #     self.status['history_profit'].append(self.monitor_profit)

        df = pd.DataFrame(self.status.copy())
        df['Deal'] = df['monitor_stop'] + df['monitor_take'] + df['monitor_trailer'] + df['monitor_time']
        df['PPT'] = df['monitor_profit'] / df['Deal']

        df.rename(columns={'monitor_stop': 'Stop',
                           'monitor_take': 'Take',
                           'monitor_trailer': 'Trailer',
                           'monitor_trailer_avg_time': 'Trl.Avg.Tim',
                           'monitor_time': 'Time',
                           'monitor_profit': 'Profit',
                           'monitor_max_loss': 'MaxLoss',
                           'monitor_fee': 'Fee',
                           }, inplace=True)

        np.save("trailer_times", np.array(self.monitor_trailer_deal_times))
        return df

    def open_pnl(self):
        pnl = 0
        for slot in self._slot_position:
            if self._slot_position[slot]['qty'] > 0:
                income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
                exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[self._slot_position[slot]['symbol']]
                pnl += round(income_value - exit_value, 2)
        return pnl
    
    def slot_in_position(self, slot):
        if self._slot_position[slot]['qty'] > 0:
            return True
        elif self._slot_position[slot]['qty'] == 0:
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

    def defa_symbol_array_l_zero(self, l):
        ret = {}
        for sy in self._traded_symbols:
            ret[sy] = [0] * l
        return ret

    def defa_symbol_array(self):
        ret = {}
        for sy in self._traded_symbols:
            ret[sy] = []
        return ret

    def build_ai(self):
        for sl in self.slot:

            tf_model_name = self.slot[sl]['tf_model_file']
            model = load_model(tf_model_name)
            self.models[sl] = LiteModel.from_keras_model(model)

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

    async def asyc_websocket_orderbook(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            # i_socket_list.append(self.get_socket_name(symbol, "trade"))
            i_socket_list.append(self.get_socket_name(symbol, "depth20"))
            # i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        async with self.ts as tscm:
            while True:
                res = await tscm.recv()
                # print(res)
                symbol = res['stream'].split('@')[0].upper()
                # stype = res['stream'].split('@')[1]
                # print(datetime.now(), res)
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
                # if len(self.actual_traded_price[symbol]) >= 3:
                #     calc_base_price = np.sum(self.actual_traded_price[symbol][-3:]) / 3
                # else:
                #     calc_base_price = self.actual_traded_price[symbol][-1]

                calc_base_price = np.sum(self.actual_traded_price[symbol][-3:]) / 3
                self.base_prices[symbol].append(calc_base_price)
                if len(self.base_prices[symbol]) > self.max_depths[symbol]:
                    self.base_prices[symbol] = self.base_prices[symbol][1:]

                self.orderbooks[symbol].append(res['data'])
                self.orderbooks[symbol] = self.orderbooks[symbol][-3:]
                # if len(self.orderbooks[symbol]) > self.max_depths[symbol]:

                for slot in self._symbol_slot[symbol]:
                    await self.trader(symbol, slot, self.orderbooks[symbol], self.base_prices[symbol])

    async def asyc_websocket_trade(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "trade"))
            # i_socket_list.append(self.get_socket_name(symbol, "depth20"))
            # i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        async with self.ts as tscm:
            while True:
                res = await tscm.recv()
                # print(res)
                symbol = res['stream'].split('@')[0].upper()
                # stype = res['stream'].split('@')[1]

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
                # if len(self.actual_traded_price) > 3:
                self.actual_traded_price[symbol] = self.actual_traded_price[symbol][-3:]

    async def asyc_websocket_bookticker(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        async with self.ts as tscm:
            while True:
                res = await tscm.recv()
                # print(res)
                symbol = res['stream'].split('@')[0].upper()
                # stype = res['stream'].split('@')[1]

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

                if self.total_symbol_position[symbol] > 0:
                    for slot in self._symbol_slot[symbol]:
                        if self._slot_position[slot]['qty'] > 0:

                            loss = ((self.actual_bid_price[symbol] / self._slot_position[slot]['income_price']) - 1) * 100
                            self.monitor_max_loss[slot] = min(self.monitor_max_loss[slot], loss)

                            act_traile_price = self.actual_bid_price[symbol] * (1 - self.slot[slot]['trailer_delta'])
                            self._slot_position[slot]['trailer_stop_price'] = max(self._slot_position[slot]['trailer_stop_price'],
                                                                                  act_traile_price)
                            # print(self.slot_position)
                            if self.actual_bid_price[symbol] > self._slot_position[slot]['take_price']:
                                # print("stop", self.actual_bid_price[symbol], self._slot_position[slot]['take_price'])
                                self.stop(symbol, slot)
                                self.monitor_take[slot] += 1
                            elif self._slot_position[slot]['exit_dt'] < datetime.now() and not self._slot_position[slot]['extra_flag']:
                                self._slot_position[slot]['extra_flag'] = True
                                if self.actual_bid_price[symbol] < self._slot_position[slot]['income_price']:
                                    print("trailer reset")
                                    income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
                                    exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
                                    print("Actual profit at reset:", exit_value - income_value)
                                    self.reset_loss_reducer(symbol, slot)
                                else:
                                    self.stop(symbol, slot)
                                    self.monitor_time[slot] += 1
                            elif self._slot_position[slot]['extra_dt'] < datetime.now() and self._slot_position[slot]['extra_flag']:
                                self.stop(symbol, slot)
                                self.monitor_time[slot] += 1
                            elif self.actual_bid_price[symbol] < self._slot_position[slot]['stop_price']:
                                # print("    bid", self.actual_bid_price[symbol], "stop   ", self.slot_position[slot]['stop_price'])
                                self.stop(symbol, slot)
                                self.monitor_stop[slot] += 1
                            elif self._slot_position[slot]['trailer_stop_price'] > self._slot_position[slot]['trailer_minimum_price'] \
                                    and self.actual_bid_price[symbol] < self._slot_position[slot]['trailer_stop_price']:
                                # print("trailer", self.actual_bid_price[symbol], self._slot_position[slot]['trailer_stop_price'])
                                self.monitor_trailer[slot] += 1
                                deal_time = datetime.now() - self._slot_position[slot]['enter_dt']
                                deal_time = float(deal_time.total_seconds())
                                self.monitor_trailer_avg_time[slot] = ((self.monitor_trailer_avg_time[slot] * (self.monitor_trailer[slot] - 1)) + deal_time) / self.monitor_trailer[slot]
                                self.stop(symbol, slot)
                                self.monitor_trailer_deal_times.append(deal_time)

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

            # x = np.zeros((1, px.shape[0], px.shape[1], px.shape[2]), dtype=np.float32)
            # x[0] = px

            # dt = datetime.now()
            # y_resulto = self.ori_ft.predict(x, verbose=0, batch_size=256)
            # y_result = self.models[slot].predict(x)
            y_result = [self.models[slot].predict_single(px)]
            # print(y_resulto)
            # print(y_result)
            # print(datetime.now() - dt)

            # print(datetime.now(), y_result)

            y_predict = np.argmax(y_result, axis=1)
            y_filter = self.slot[slot]['y_filter']

            if y_predict[0] == 1 and y_result[0][1] > y_filter and not self.slot_in_position(slot):
                self.buy(symbol, slot, 1)

    def get_x_3d(self, orderbooks, base_prices, x_type=3, qmin=-50, qmax=50, qstep=.5, depth=3):
        if len(orderbooks) != 3:
            return [np.nan]

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

        elif x_type == 33:

            iret_pre = []
            set_max = float(0.0)
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
                set_max = max(np.sum(bids_scaled_qt), np.sum(asks_scaled_qt), set_max)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            for d in range(depth):
                iret_pre[d][0] = iret_pre[d][0] / set_max
                iret_pre[d][1] = iret_pre[d][0] / set_max

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

        elif x_type == 5:

            # az ordebok közepe a baseprice

            iret_pre = []
            base_price = (float(orderbooks[0]['bids'][0][0]) + float(orderbooks[0]['asks'][0][0])) / 2
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_price, dtype=np.float32)

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
    n_tob.strat_threads()

    while True:
        status = n_tob.get_status().copy()
        print(datetime.now(), 'Total_Position:', n_tob.total_symbol_position)
        print(status)

        # pickle.dump(self.get_status(), open("paper_trade_multy_status.pickle", "wb"))
        # print("\r lasr save:" + str(datetime.now()), end="")
        time.sleep(20)

    