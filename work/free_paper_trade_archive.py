import os
import random
import sys
import time
import pickle
# from numba import njit
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import seaborn as sns
# sns.set_theme(style="whitegrid")
import asyncio
from threading import Thread
from scipy.signal import savgol_filter, argrelextrema, find_peaks, argrelmin

from binance import AsyncClient, BinanceSocketManager, Client
from binance.exceptions import BinanceAPIException
from binance.enums import *

# from nDot_crypto_db_connector import nDot_db_connector

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
# from tensorflow.keras.models import load_model

import tensorflow as tf
from sklearn import linear_model
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import LinearRegression

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
        self.ntick = .5 / 15000
        self.time_period = 20000
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
            #Settings
            'BTCUSDT_SX3': {'trade_symbol': 'BTCBUSD',
                            'y_filter': .82,
                            'x_type': 3,
                            'depth': 3,
                            'qmin': -50,
                            'qmax': 50,
                            'qstep': .5,
                            'stop_delta': self.ntick * 6,
                            'trailer_delta': self.ntick * 3.2,
                            'take_delta': self.ntick * 6,
                            'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
                            'max_time_sec': 60 * 120,
                            'extra_time_sec': 60 * 1,
                            'extra_stop_delta': .0006 / 100,
                            'extra_trailer_delta': .0006 / 100,
                            'extra_take_delta': 9.9 / 100,
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

        self.actual_buy_qty = 0.01
        self.qty_rise_flag = False

        self.best_bid_price_history = np.array([0.0] * self.time_period)
        self.best_ask_price_history = np.array([0.0] * self.time_period)
        self.bid_ask_spread = np.array([0.0] * self.time_period)
        self.bid_ask_spread_avg = np.array([0.0] * self.time_period)
        self.renko_price_history = np.array([0.0] * self.time_period)
        self.renko_fast_price_history = np.array([0.0] * self.time_period)
        self.best_mid_price_history_ma_fast = np.array([0.0] * self.time_period)
        self.best_mid_price_history_ma_slow = np.array([0.0] * self.time_period)
        self.smoot_price_history = np.array([0.0] * self.time_period)
        self.smoot_fast_price_history = np.array([0.0] * self.time_period)
        self.best_smoot2_price_history = np.array([0.0] * self.time_period)
        self.best_smoot3_price_history = np.array([0.0] * self.time_period)
        self.best_bid_qty_history = np.array([0.0] * self.time_period)
        self.best_ask_qty_history = np.array([0.0] * self.time_period)
        # self.qty_way_history = np.array([0.0] * self.time_period)
        self.traded_price_history = [0.0] * self.time_period
        self.decision_neutral = 500
        self.decision_long = 600
        self.decision_short = 400
        self.decision_history = [self.decision_neutral] * self.time_period
        self.market_speed_array = np.array([0.0] * self.time_period)
        self.market_speed_array_avg = np.array([0.0] * self.time_period)
        self.market_speed_stamp = datetime.now()
        # self.market_speed = 2
        self.min_market_speed = 0
        self.transfer = np.empty((11, self.time_period))
        self.transfer_status = np.empty((10))

        # plot
        self.ylim_max = 0
        self.ylim_min = 0
        self.xaxis = np.arange(0, self.renko_price_history.shape[0])
        self.xaxis = self.xaxis.reshape((-1, 1))

        self.trend = ""
        self.last_traded_price = 0
        self.c = 0

        self.project_name = 'LOB'
        self.models = {}
        # self.build_ai()
        self.orderbooks = self.defa_symbol_array()
        self.base_prices = self.defa_symbol_array()

        # trade monitoring
        self.monitor_stop = self.defa_slot_zero()
        self.monitor_trailer = self.defa_slot_zero()
        self.monitor_trailer_avg_time = self.defa_slot_zero()
        self.monitor_take = self.defa_slot_zero()
        self.monitor_time = self.defa_slot_zero()
        self.monitor_profit = self.defa_slot_zero()
        self.monitor_profit_short = self.defa_slot_zero()
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
        # task3 = Thread(target=self.orderbook_thr, args=[])
        task4 = Thread(target=self.bookticker_trend_thr, args=[])
        # task5 = Thread(target=self.deal_hunter_thr, args=[])
        task6 = Thread(target=self.print_monitor, args=[])

        task1.start()
        time.sleep(2)

        # task2.start()
        # time.sleep(1)
        task4.start()
        time.sleep(3)

        # Data plotter

        # self.best_mid_price_history[self.best_mid_price_history == 0] = self.best_mid_price_history[-1]
        # self.best_bid_price_history[self.best_bid_price_history == 0] = self.best_bid_price_history[-1]
        # self.best_ask_price_history[self.best_ask_price_history == 0] = self.best_ask_price_history[-1]
        # self.best_smoot_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.best_smoot2_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.best_smoot3_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.market_speed_array[self.market_speed_array > 5] = 0
        #
        # time.sleep(5)
        # task3.start()
        # task5.start()
        task6.start()
        #
        # self.limdif = .0005
        # fig, ax = plt.subplots(3, 1,
        #                        gridspec_kw={'height_ratios': [6, 1, 1]},
        #                        figsize=(17, 6))
        # # fig.canvas.manager.window.move(50, 250)
        #
        # self.ax1 = fig.add_subplot(3, 1, 1)
        # self.ax2 = fig.add_subplot(3, 1, 2)
        # self.ax3 = fig.add_subplot(3, 1, 3)
        # ani = animation.FuncAnimation(fig, self.animate_plot, interval=750)
        # plt.autoscale(False)
        # ax[0].set_xticks([])
        # ax[0].set_yticks([])
        # ax[1].set_xticks([])
        # ax[1].set_yticks([])
        # ax[2].set_xticks([])
        # ax[2].set_yticks([])
        # # plt.gca().ticklabel_format(axis='y', style='plain')
        # # self.ylim_min = self.best_bid_price_history[-1] * (1 - self.limdif)
        # # self.ylim_max = self.best_ask_price_history[-1] * (1 + self.limdif)
        # # self.ax1.set_ylim(self.ylim_min, self.ylim_max)
        # plt.subplots_adjust(left=0.05, right=1, top=.996, bottom=0.05, hspace=0)
        # # plt.tight_layout()
        # plt.show()

    def print_monitor(self):
        while True:
            status = self.get_status().copy()

            self.transfer[0] = np.array(self.best_bid_price_history)
            self.transfer[1] = np.array(self.best_ask_price_history)
            self.transfer[2] = np.array(self.smoot_price_history)
            self.transfer[3] = np.array(self.decision_history)
            self.transfer[4] = np.array(self.market_speed_array_avg)
            self.transfer[5] = np.array(self.market_speed_array)
            self.transfer[6] = np.array(self.bid_ask_spread_avg)
            self.transfer[7] = np.array(self.bid_ask_spread)
            self.transfer[8] = np.array(self.best_ask_qty_history)
            self.transfer[9] = np.array(self.best_bid_qty_history)
            self.transfer[10] = np.array(self.smoot_fast_price_history)
            np.save("../transfer.npy", self.transfer)

            f = open("../transfer_status.pkl", "wb")
            pickle.dump(self._slot_position, f)
            f.close()

            print(status)
            time.sleep(7)

    def moving_average(self, x, w):
        iret = np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])
        return iret

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

    def bookticker_trend_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_bookticker_trend())
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
                                       'income_price_short': 0,
                                       'stop_price': 0,
                                       'trailer_stop_price': 0,
                                       'trailer_minimum_price': 0,
                                       'take_price': 0,
                                       'enter_dt': None,
                                       'exit_dt': None,
                                       'extra_dt': None,
                                       'extra_flag': False
                                       }

    def buy(self, symbol, slot):
        if not self.qty_rise_flag:
            # print("")
            # print("speed:", self.market_speed_array[-1],
            #       "spread:", self.bid_ask_spread[-1],
            #       "bid qty:", self.best_bid_qty_history[-1],
            #       "ask qty:", self.best_ask_qty_history[-1])

            self._slot_position[slot]['symbol'] = symbol
            existed_value = self._slot_position[slot]['income_price'] * self._slot_position[slot]['qty']
            new_value = self.actual_buy_qty * self.actual_ask_price[symbol]

            print(f"Buy: existed_value: {existed_value} new_value: {new_value} act_ask_price: {self.actual_ask_price[symbol]} act_income_price:{self._slot_position[slot]['income_price']}")
            self._slot_position[slot]['qty'] += self.actual_buy_qty  # new qty
            self._slot_position[slot]['income_price'] = (existed_value + new_value) / self._slot_position[slot]['qty']
            print(f"actual_income_price: {self._slot_position[slot]['income_price']} actual_qty:{self._slot_position[slot]['qty']}")
            self._slot_position[slot]['income_price_short'] = self.actual_bid_price[symbol]
            # print("income price:", self._slot_position[slot]['income_price'])
            self._slot_position[slot]['stop_price'] = self._slot_position[slot]['income_price'] * (1 - self.slot[slot]['stop_delta'])
            self._slot_position[slot]['trailer_stop_price'] = self._slot_position[slot]['income_price']  # innen indul és kezdi emelgetni
            self._slot_position[slot]['trailer_minimum_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['trailer_delta'])
            self._slot_position[slot]['take_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['take_delta'])
            self._slot_position[slot]['enter_dt'] = datetime.now()
            self._slot_position[slot]['exit_dt'] = self._slot_position[slot]['enter_dt'] + timedelta(seconds=self.slot[slot]['max_time_sec'])
            self._slot_position[slot]['extra_dt'] = self._slot_position[slot]['exit_dt'] + timedelta(seconds=self.slot[slot]['extra_time_sec'])
            self._slot_position[slot]['extra_flag'] = False
            self.total_symbol_position[symbol] += self.actual_buy_qty
            self.decision_history.pop(0)
            self.decision_history.append(self.decision_long)
            self.qty_rise_flag = True
        else:
            self.decision_history.pop(0)
            self.decision_history.append(self.decision_neutral)
            # print("")
            # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def stop(self, symbol, slot, message=""):

        # time.sleep(.01)
        if self._slot_position[slot]['income_price'] < self.actual_bid_price[symbol]:
            print(message)
            income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
            income_value_short = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price_short']
            exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
            exit_value_short = self._slot_position[slot]['qty'] * self.actual_ask_price[symbol]
            print("profit:", exit_value - income_value)
            self.monitor_profit[slot] += (exit_value - income_value)
            self.monitor_profit_short[slot] += (income_value_short - exit_value_short)
            # print("Close profit:", (exit_value - income_value))
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
            self.decision_history.pop(0)
            self.decision_history.append(self.decision_short)
            self.actual_buy_qty = .01
            self.qty_rise_flag = False
        elif self.qty_rise_flag:
            print(message)
            self.decision_history.pop(0)
            self.decision_history.append(int(self.decision_short / 2))
            self.actual_buy_qty += 0
            print("itt", self.actual_buy_qty)
            self.qty_rise_flag = False
        else:
            self.decision_history.pop(0)
            self.decision_history.append(self.decision_neutral)

    def get_status(self):

        self.status['monitor_stop'] = self.monitor_stop
        self.status['monitor_take'] = self.monitor_take
        self.status['monitor_trailer'] = self.monitor_trailer
        self.status['monitor_trailer_avg_time'] = self.monitor_trailer_avg_time
        self.status['monitor_time'] = self.monitor_time
        self.status['monitor_profit'] = self.monitor_profit
        self.status['monitor_profit_short'] = self.monitor_profit_short
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
                           'monitor_profit_short': 'ProfitSH',
                           'monitor_max_loss': 'MaxLoss',
                           'monitor_fee': 'Fee',
                           }, inplace=True)

        # np.save("trailer_times", np.array(self.monitor_trailer_deal_times))
        return df

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

    # def build_ai(self):
    #     for sl in self.slot:
    #
    #         tf_model_name = self.slot[sl]['tf_model_file']
    #         model = load_model(tf_model_name)
    #         self.models[sl] = LiteModel.from_keras_model(model)

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
                    if self.total_symbol_position[symbol] == 0:
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
                self.last_traded_price = float(res['data']['p'])
                # if len(self.actual_traded_price) > 3:
                self.actual_traded_price[symbol] = self.actual_traded_price[symbol][-3:]

    def trough_detect(self, ts):
        if ts[-1] > ts[-2]:
            return True
        else:
            return False

    async def asyc_websocket_bookticker_trend(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        self.market_speed_stamp = datetime.now()

        r1 = -10
        r2 = -40
        r3 = -3000

        ddown_points = np.arange(r3, -1, 20)

        async with self.ts as tscm:
            while True:
                res = await tscm.recv()
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
                bid_qty = float(res['data']['B'])
                ask = float(res['data']['a'])
                ask_qty = float(res['data']['A'])

                self.best_bid_price_history = np.delete(np.append(self.best_bid_price_history, [bid], axis=0), 0)
                # self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [(np.sum(self.best_bid_qty_history[-10:]) + np.max((bid_qty * -100, -500))) / 11], axis=0), 0)
                self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [np.max((bid_qty * -100, -500))], axis=0), 0)
                self.best_ask_price_history = np.delete(np.append(self.best_ask_price_history, [ask], axis=0), 0)
                # self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [(np.sum(self.best_ask_qty_history[-10:]) + np.min((ask_qty * 100, 500))) / 11], axis=0), 0)
                self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [np.min((ask_qty * 100, 500))], axis=0), 0)
                # self.qty_way_history = np.delete(np.append(self.qty_way_history, [np.sum(self.best_ask_qty_history[-250:]) + np.sum(self.best_bid_qty_history[-250:])], axis=0), 0)
                self.bid_ask_spread = np.delete(np.append(self.bid_ask_spread, [(ask - bid) * 1000], axis=0), 0)
                self.bid_ask_spread_avg = np.delete(np.append(self.bid_ask_spread_avg, [np.sum(self.bid_ask_spread[-10:]) / 10], axis=0), 0)
                renko_price = (bid + ask) / 2
                if 2.12 <= abs(self.renko_price_history[-1] - renko_price):
                    self.renko_price_history = np.delete(np.append(self.renko_price_history, [renko_price], axis=0), 0)
                else:
                    self.renko_price_history = np.delete(np.append(self.renko_price_history, [self.renko_price_history[-1]], axis=0), 0)

                if .5 <= abs(self.renko_fast_price_history[-1] - renko_price):
                    self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [renko_price], axis=0), 0)
                else:
                    self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [self.renko_fast_price_history[-1]], axis=0), 0)


                # self.renko_price_history = np.delete(np.append(self.renko_price_history, [(bid + ask) / 2], axis=0), 0)

                # self.best_smoot2_price_history = np.delete(np.append(self.best_smoot2_price_history, [np.sum(self.best_ask_price_history[-50:]) / 50], axis=0), 0)
                # self.best_smoot3_price_history = np.delete(np.append(self.best_smoot3_price_history, [np.sum(self.best_smoot2_price_history[-150:]) / 150], axis=0), 0)
                # self.best_smoot_price_history = np.delete(np.append(self.best_smoot_price_history, [np.sum(self.best_smoot3_price_history[-50:]) / 50], axis=0), 0)

                self.smoot_price_history = savgol_filter(self.renko_price_history, 100, 1)  # window size 51, polynomial order 3
                self.smoot_fast_price_history = savgol_filter(self.renko_fast_price_history, 100, 1)  # window size 51, polynomial order 3
                # self.best_smoot_price_history = self.renko_price_history


                # self.best_mid_price_history_ma_fast = np.delete(np.append(self.best_mid_price_history_ma_fast, [np.sum(self.best_smoot_price_history[-5:]) / 5], axis=0), 0)
                # self.best_mid_price_history_ma_slow = np.delete(np.append(self.best_mid_price_history_ma_slow, [np.sum(self.best_smoot_price_history[-10:]) / 10], axis=0), 0)


                # self.best_ask_qty_history.pop(0)
                # self.best_ask_qty_history.append(float(res['data']['A']))

                # self.traded_price_history.pop(0)
                # self.traded_price_history.append(self.last_traded_price)

                # if (self.best_mid_price_history_ma_fast[-1] > self.best_mid_price_history_ma_slow[-1]) and \
                #         np.min(self.best_mid_price_history_ma_slow[-20:-3] - self.best_mid_price_history_ma_fast[-20:-3]) > 0 and \
                #         (self.best_mid_price_history_ma_fast[-150] - self.best_mid_price_history_ma_fast[-2]) > 1.5 and \
                #         not self.slot_in_position('BTCUSDT_SX3'):


                # d1 = self.best_smoot_price_history[r3:r2].copy()
                # d1 = np.delete(d1, last)
                # d2 = self.best_smoot_price_history[r3:r2].copy()
                # d2 = np.delete(d2, 0)
                # dif = np.min(d1 - d2)
                # print(np.min(d1-d2))

                c = datetime.now() - self.market_speed_stamp
                c = c.total_seconds() * 1000 * 10
                self.market_speed_array = np.delete(np.append(self.market_speed_array, [c], axis=0), 0)
                self.market_speed_array_avg = np.delete(np.append(self.market_speed_array_avg, [np.sum(self.market_speed_array[-10:]) / 10], axis=0), 0)
                self.market_speed_stamp = datetime.now()

                ddown = self.smoot_price_history[ddown_points] - self.smoot_price_history[-1]
                ddown = ddown > 4.444
                # print(self.best_smoot_price_history[-100:] * -1)
                # print(argrelmin(self.best_smoot_price_history[-1000:] * -1))

                # a = argrelmin(self.best_smoot_price_history[-1000:])[0]
                # print(a)

                # if len(a) > 0:
                #     print(a)

                # if ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):
                # if self.trough_detect(self.smoot_fast_price_history) and ddown.any() and np.max(self.decision_history[-1500:]) == np.min(self.decision_history[-1500:]) == self.decision_neutral\
                #         and not self.slot_in_position('BTCUSDT_SX3'):
                if self.trough_detect(self.smoot_fast_price_history) and ddown.any():
                # if len(a) > 0 and ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):

                # if self.best_smoot_price_history[-10] > self.best_smoot_price_history[-11] < self.best_smoot_price_history[-12] and \
                #     not self.slot_in_position('BTCUSDT_SX3'):

                # np.min(self.best_smoot_price_history[r3:r2] - self.best_smoot_price_history[r2 + 1]) >= 0 and \
                # ddown.any() and \


                    # self.min_market_speed = max((self.min_market_speed, self.bid_ask_spread[-1]))
                # print(self.min_market_speed)

                    # self.bid_ask_spread[-1] < 700 and \
                # if self.market_speed_array[-1] < 7 and \
                #     self.qty_way_history[-1] > 1800 and \
                #     self.bid_ask_spread[-1] > 500 and \
                # if self.bid_ask_spread[-1] > 820 and \
                #     not self.slot_in_position('BTCUSDT_SX3'):
                    # (self.best_smoot_price_history[r3] - self.best_smoot_price_history[r2]) > self.ntick * 1 and \
                    # dif > 0 and \
                    # self.market_speed_array_avg[-1] < 2 and \
                    #     self.bid_ask_spread_avg[-1] > 200 and \
                    # (self.best_smoot_price_history[-150] - self.best_smoot_price_history[-15]) > self.ntick * 6 and \

                    # self.decision_history.pop(0)
                    # self.decision_history.append(self.decision_long)

                    symbol = self.slot['BTCUSDT_SX3']['trade_symbol']
                    self.buy(symbol, 'BTCUSDT_SX3')
                else:
                    self.decision_history.pop(0)
                    self.decision_history.append(self.decision_neutral)

                # self.market_speed = np.mean(np.array(self.market_speed_array[-100:]))

    async def asyc_websocket_bookticker(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)
        slot = 'BTCUSDT_SX3'

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
                # self.actual_bid_qty[symbol] = float(res['data']['B'])

                self.actual_ask_price[symbol] = float(res['data']['a'])
                # self.actual_ask_qty[symbol] = float(res['data']['A'])

                # Stopper

                # synthetic_price = self.actual_bid_price[symbol]
                synthetic_price = self.actual_bid_price[symbol]

                if self._slot_position[slot]['qty'] > 0:

                    # act_trailer_price = synthetic_price
                    act_trailer_price = synthetic_price * (1 - self.slot[slot]['trailer_delta'])
                    self._slot_position[slot]['trailer_stop_price'] = max(self._slot_position[slot]['trailer_stop_price'], act_trailer_price)

                    if synthetic_price >= self._slot_position[slot]['take_price']:
                        # print("stop", self.actual_bid_price[symbol], self._slot_position[slot]['take_price'])
                        # self._slot_position[slot]['stop_price'] = self.actual_bid_price[symbol]
                        self.stop(symbol, slot, "Stop: Take")
                        self.monitor_take[slot] += 1
                        # if self.actual_bid_price[symbol] - self._slot_position[slot]['income_price'] > 2:
                        #     self._slot_position[slot]['stop_price'] = (self._slot_position[slot]['income_price'] + self.actual_bid_price[symbol]) / 5 * 4
                        # else:
                        #     self._slot_position[slot]['stop_price'] = (self._slot_position[slot]['income_price'] + self.actual_bid_price[symbol]) / 2
                        # self._slot_position[slot]['take_price'] = self.actual_bid_price[symbol] * (1 + self.slot[slot]['take_delta'])

                    elif synthetic_price <= self._slot_position[slot]['stop_price']:
                        # print("    bid", self.actual_bid_price[symbol], "stop   ", self.slot_position[slot]['stop_price'])
                        self.stop(symbol, slot, "Stop: Stop loss")
                        self.monitor_stop[slot] += 1
                    elif self._slot_position[slot]['exit_dt'] < datetime.now():
                        if synthetic_price > self._slot_position[slot]['income_price']:
                            self._slot_position[slot]['exit_dt'] = self._slot_position[slot]['exit_dt'] + timedelta(seconds=self.slot[slot]['extra_time_sec'])

                        # and not self._slot_position[slot]['extra_flag']:
                        # self._slot_position[slot]['extra_flag'] = True
                        # if self.actual_bid_price[symbol] > self._slot_position[slot]['income_price']:
                        #     income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
                        #     exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
                        #     print("Actual profit at reset:", exit_value - income_value)
                        #     self.reset_extra_profit(symbol, slot)
                        else:
                            self.stop(symbol, slot, "Stop: Time")
                            self.monitor_time[slot] += 1
                    elif self._slot_position[slot]['trailer_minimum_price'] <= synthetic_price <= self._slot_position[slot]['trailer_stop_price']:
                        self.monitor_trailer[slot] += 1
                        self.stop(symbol, slot, "Stop: Trailer")

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
            # if self.trend == "UP":
            #     self.decision_history.pop(0)
            #     self.decision_history.append(1)
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
        time.sleep(100)

