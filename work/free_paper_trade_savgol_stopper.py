# import os
import sys
import time
import pickle
import numpy as np
import pandas as pd
from math import factorial
from datetime import datetime, timedelta
import asyncio
from threading import Thread
from scipy.signal import savgol_filter

# from numba import int32, float32
# from numba import types, typed
# from numba.experimental import jitclass
# from numba import jit

from binance import AsyncClient, BinanceSocketManager, Client
# from binance.exceptions import BinanceAPIException
# from binance.enums import *

# from sklearn import linear_model
# from sklearn.preprocessing import PolynomialFeatures
# from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import LinearRegression

np.set_printoptions(threshold=5000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', '{:8,.4f}'.format)
pd.set_option('display.max_colwidth', None)


# spec = [
#     ('best_bid_price_history', float32[:]),
#     ('best_ask_price_history', float32[:]),
#     ('smoot_slow_price_history', float32[:]),
#     ('smoot_fast_price_history', float32[:]),
#     ('renko_slow_price_history', float32[:]),
#     ('renko_fast_price_history', float32[:]),
#     ('decision_history', int32[:]),
#     ('actual_bid_price', float32),
#     ('actual_ask_price', float32),
#     ('actual_bid_qty', float32),
#     ('actual_ask_qty', float32),
#     ('renko_slow_steps', float32),
#     ('renko_fast_steps', float32),
#     ('time_period', int32),
#     ('traded_crypto', types.string),
#     ('slot', types.DictType(types.string, float32)),
# ]
#
#
# @jitclass(spec)
# class ntrade_numba():
#     def __init__(self):
#         self.time_period = 20000
#         self.best_bid_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#         self.best_ask_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#         self.smoot_slow_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#         self.smoot_fast_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#
#         self.renko_slow_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#         self.renko_fast_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
#
#         self.decision_history = np.array([0] * self.time_period, dtype=np.int32)
#
#         self.actual_bid_price = 0.0
#         self.actual_ask_price = 0.0
#         self.actual_bid_qty = 0.0
#         self.actual_ask_qty = 0.0
#
#         self.renko_slow_steps = 2.0
#         self.renko_fast_steps = 0.25
#
#         self.traded_crypto = 'BTCUSDT'
#
#         self.slot = typed.Dict.empty(types.string, float32)
#         self.defa_slot()
#
#     def defa_slot(self):
#         self.slot = {   'max_qty': 0.0,
#                         'stop_delta': 0.0,
#                         'trailer_delta': 0.0,
#                         'take_delta': 0.0,
#                         'max_time_sec': 0,
#                         'extra_time_sec': 0,
#                         }
#
#         print(self.slot['max_qty'])
#
#     def add_data(self, bid, ask):
#         self.actual_bid_price = bid
#         self.actual_ask_price = ask
#
#         self.best_bid_price_history = np.delete(np.append(self.best_bid_price_history, np.array([self.actual_bid_price]), axis=0), 0)
#         self.best_ask_price_history = np.delete(np.append(self.best_ask_price_history, np.array([self.actual_ask_price]), axis=0), 0)
#
#         renko_price = self.actual_ask_price
#         if self.renko_slow_steps <= abs(self.renko_slow_price_history[-1] - renko_price):
#             self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, np.array([renko_price]), axis=0), 0)
#         else:
#             self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, np.array([self.renko_slow_price_history[-1]]), axis=0), 0)
#
#         if self.renko_fast_steps <= abs(self.renko_fast_price_history[-1] - renko_price):
#             self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, np.array([renko_price]), axis=0), 0)
#         else:
#             self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, np.array([self.renko_fast_price_history[-1]]), axis=0), 0)
#
#         self.smoot_slow_price_history = self.renko_slow_price_history
#         self.smoot_fast_price_history = self.renko_fast_price_history
#
#     @staticmethod
#     def trough_detect(ts):
#         if ts[-1] > ts[-2]:
#             return True
#         else:
#             return False
#
#     def set_profile(self, p):
#         if p != self.actual_profile:
#             self.slot['stop_delta'] = self.trading_profile[p]['stop_delta']
#             self.slot['trailer_delta'] = self.trading_profile[p]['trailer_delta']
#             self.slot['take_delta'] = self.trading_profile[p]['take_delta']
#             self.slot['max_time_sec'] = self.trading_profile[p]['max_time_sec']
#             self.slot['extra_time_sec'] = self.trading_profile[p]['extra_time_sec']
#
#             self.renko_slow_steps = self.trading_profile[p]['renko_slow_steps']
#             self.renko_fast_steps = self.trading_profile[p]['renko_fast_steps']
#             self.ddown_limit = self.trading_profile[p]['ddown_limit']
#             self.ddown_points = np.arange(self.trading_profile[p]['ddown_depth'], -1, 20)
#             self.actual_profile = p
#             self._slot_position['BTCUSDT_SX3']['actual_profile'] = p
#
#     def detect(self):
#
#         ddown = self.smoot_slow_price_history[self.ddown_points] - self.smoot_slow_price_history[-1]
#         ddown = ddown > self.ddown_limit
#
#         if self.trough_detect(self.smoot_fast_price_history) and ddown.any():
#             # if len(a) > 0 and ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):
#             # if self.best_smoot_price_history[-10] > self.best_smoot_price_history[-11] < self.best_smoot_price_history[-12] and \
#             #     not self.slot_in_position('BTCUSDT_SX3'):
#
#             # self.decision_history.pop(0)
#             # self.decision_history.append(self.decision_long)
#
#             symbol = self.slot['BTCUSDT_SX3']['trade_symbol']
#             self.buy(symbol, 'BTCUSDT_SX3')
#         else:
#             self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)


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
            # 'BTCUSDT_SX3': {'trade_symbol': 'BTCBUSD',
            #                 'y_filter': .82,
            #                 'x_type': 3,
            #                 'depth': 3,
            #                 'qmin': -50,
            #                 'qmax': 50,
            #                 'qstep': .5,
            #                 'stop_delta': self.ntick * 6,
            #                 'trailer_delta': self.ntick * 3,
            #                 'take_delta': self.ntick * 12,
            #                 'tf_model_file': f"projects/LOB/BTCUSDT/nDot_TF_MODEL_LOB_BTCUSDT.h5",
            #                 'max_time_sec': 60 * 120,
            #                 'extra_time_sec': 60 * 1,
            #                 'extra_stop_delta': .0006 / 100,
            #                 'extra_trailer_delta': .0006 / 100,
            #                 'extra_take_delta': 9.9 / 100,
            #                 }
            'BTCUSDT_SX3': {'trade_symbol': 'BTCBUSD',
                            'max_qty': 0.15,
                            'stop_delta': 0.0,
                            'trailer_delta': 0.0,
                            'take_delta': 0.0,
                            'max_time_sec': 0,
                            'extra_time_sec': 0,
                            }
        }

        self.trading_profile = {
            1: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 120,
                'extra_time_sec': 60 * 1,
                'renko_slow_steps': 1,
                'renko_fast_steps': .25,
                'renko_stop_steps': .75,
                'ddown_limit': 1.9,
                'ddown_depth': -2500,
                },
            2: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 120,
                'extra_time_sec': 60 * 1,
                'renko_slow_steps': 2,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': 9.9,
                'ddown_depth': -4500,
            },
            3: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 120,
                'extra_time_sec': 60 * 1,
                'renko_slow_steps': 5,
                'renko_fast_steps': .7,
                'renko_stop_steps': .75,
                'ddown_limit': 14.9,
                'ddown_depth': -4500,
            },
        }

        self.profile_activator = [.018, .056]

        self._traded_symbols = []
        self._slots = []
        self._symbol_slot = {}
        self._slot_position = {}
        self._defa()

        # egy adott symbolbol mennyi van
        # csak akkor követia stop lost és a trailert ha van belőle
        # ez pozíció vezetésre nem szolgál csak technikai gyorsításra, hogy ne kellejen kalkulálni
        self.total_symbol_position = self.defa_symbol_zero()

        # self.max_depths = {'ETHUSDT': 3,
        #                'BTCUSDT': 4,
        #                'SOLUSDT': 3}

        # utolsó piaci kötésvan benne
        # ez kell az get_x_3d normálásához
        self.actual_traded_price = self.defa_symbol_array()

        # utolsó simert orderbook legjob datai
        # ez kell a buy és a stop hoz
        self.actual_bid_price = self.defa_symbol_zero()
        self.actual_ask_price = self.defa_symbol_zero()
        self.actual_bid_qty = self.defa_symbol_zero()
        self.actual_ask_qty = self.defa_symbol_zero()

        # self.renko_slow_steps = 1.75
        # self.renko_fast_steps = .5

        self.renko_slow_steps = 0.0
        self.renko_fast_steps = 0.0
        self.renko_stop_steps = 0.0
        self.ddown_limit = 0.0
        self.ddown_points = np.array([])

        self.start_buy_qty = 0.0001  # BTC
        self.buy_multiplier = 1.1
        self.actual_buy_qty = self.start_buy_qty
        self.qty_rise_flag = False

        self.best_bid_price_history = np.array([0.0] * self.time_period)
        self.best_ask_price_history = np.array([0.0] * self.time_period)
        self.bid_ask_spread = np.array([0.0] * self.time_period)
        self.bid_ask_spread_avg = np.array([0.0] * self.time_period)
        self.renko_slow_price_history = np.array([0.0] * self.time_period)
        self.renko_fast_price_history = np.array([0.0] * self.time_period)
        self.renko_stop_price_history = np.array([0.0] * self.time_period)
        # self.best_mid_price_history_ma_fast = np.array([0.0] * self.time_period)
        # self.best_mid_price_history_ma_slow = np.array([0.0] * self.time_period)
        self.smoot_slow_price_history = np.array([0.0] * self.time_period)
        self.smoot_fast_price_history = np.array([0.0] * self.time_period)
        # self.best_smoot2_price_history = np.array([0.0] * self.time_period)
        # self.best_smoot3_price_history = np.array([0.0] * self.time_period)
        # self.best_bid_qty_history = np.array([0.0] * self.time_period)
        # self.best_ask_qty_history = np.array([0.0] * self.time_period)
        # self.qty_way_history = np.array([0.0] * self.time_period)
        self.traded_price_history = [0.0] * self.time_period

        self.decision_neutral = 0
        self.decision_long = 500
        self.decision_stop = -500
        self.decision_rebuy = 250
        self.decision_history = np.array([self.decision_neutral] * self.time_period)
        self.market_speed_array = np.array([0.0] * self.time_period)

        # self.market_speed_array_avg = np.array([0.0] * self.time_period)
        # self.market_speed_stamp = datetime.now()
        # self.market_speed = 2
        # self.min_market_speed = 0

        self.transfer = np.empty((11, self.time_period))
        self.transfer_status = np.empty((10))

        # self.trend = ""
        # self.last_traded_price = 0
        # self.c = 0

        self.project_name = 'LOB'
        # self.models = {}
        # self.build_ai()
        # self.orderbooks = self.defa_symbol_array()
        # self.base_prices = self.defa_symbol_array()

        # trade monitoring
        self.monitor_stop = self.defa_slot_zero()
        self.monitor_trailer = self.defa_slot_zero()
        # self.monitor_trailer_avg_time = self.defa_slot_zero()
        self.monitor_take = self.defa_slot_zero()
        self.monitor_time = self.defa_slot_zero()
        self.monitor_rebuy = self.defa_slot_zero()
        self.monitor_profit = self.defa_slot_zero()
        self.monitor_profit_short = self.defa_slot_zero()
        self.monitor_max_qty = self.defa_slot_zero()
        self.monitor_min_value = self.defa_slot_zero()
        # self.monitor_max_loss = self.defa_slot_zero()
        self.monitor_fee = self.defa_slot_zero()
        # self.monitor_trailer_deal_times = []
        self.status = {'monitor_stop': {},
                       'monitor_re_buy': {},
                       'monitor_take': {},
                       'monitor_trailer': {},
                       'monitor_time': {},
                       'monitor_profit': {},
                       'monitor_fee': {},
                       'monitor_max_qty': {},
                       'monitor_min_value': {}
                       }

        self.actual_profile = 0
        self.set_profile(1)

    def set_profile(self, p):
        if p != self.actual_profile:
            self.slot['BTCUSDT_SX3']['stop_delta'] = self.trading_profile[p]['stop_delta']
            self.slot['BTCUSDT_SX3']['trailer_delta'] = self.trading_profile[p]['trailer_delta']
            self.slot['BTCUSDT_SX3']['take_delta'] = self.trading_profile[p]['take_delta']
            self.slot['BTCUSDT_SX3']['max_time_sec'] = self.trading_profile[p]['max_time_sec']
            self.slot['BTCUSDT_SX3']['extra_time_sec'] = self.trading_profile[p]['extra_time_sec']

            self.renko_slow_steps = self.trading_profile[p]['renko_slow_steps']
            self.renko_fast_steps = self.trading_profile[p]['renko_fast_steps']
            self.renko_stop_steps = self.trading_profile[p]['renko_stop_steps']
            self.ddown_limit = self.trading_profile[p]['ddown_limit']
            self.ddown_points = np.arange(self.trading_profile[p]['ddown_depth'], -1, 20)
            self.actual_profile = p
            self._slot_position['BTCUSDT_SX3']['actual_profile'] = p

    def start_threads(self):
        task1 = Thread(target=self.bookticker_thr, args=[])
        # task2 = Thread(target=self.trade_thr, args=[])
        # task3 = Thread(target=self.orderbook_thr, args=[])
        task4 = Thread(target=self.bookticker_trend_thr, args=[])
        # task5 = Thread(target=self.deal_hunter_thr, args=[])
        task6 = Thread(target=self.data_transfer, args=[])

        task1.start()
        time.sleep(2)

        # task2.start()
        # time.sleep(1)
        task4.start()
        time.sleep(3)
        task6.start()

    def data_transfer(self):
        while True:

            self.transfer[0] = np.array(self.best_bid_price_history)
            self.transfer[1] = np.array(self.best_ask_price_history)
            self.transfer[2] = np.array(self.smoot_slow_price_history)
            self.transfer[3] = np.array(self.decision_history)
            # self.transfer[4] = np.array(self.market_speed_array_avg)
            # self.transfer[5] = np.array(self.market_speed_array)
            # self.transfer[6] = np.array(self.bid_ask_spread_avg)
            # self.transfer[7] = np.array(self.bid_ask_spread)
            # self.transfer[8] = np.array(self.best_ask_qty_history)
            # self.transfer[9] = np.array(self.best_bid_qty_history)
            self.transfer[10] = np.array(self.smoot_fast_price_history)
            np.save("transfer_timeseries.npy", self.transfer)

            f = open("transfer_position.pkl", "wb")
            pickle.dump(self._slot_position, f)
            f.close()

            status = self.get_statistics()
            f = open("transfer_statistics.pkl", "wb")
            pickle.dump(status, f)
            f.close()

            # self.print_statistics(status)
            time.sleep(7)

    # def moving_average(self, x, w):
    #     iret = np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])
    #     return iret

    def bookticker_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_bookticker_stopper())
        loop.close()

    def bookticker_trend_thr(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket_bookticker_detect())
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
                                       'extra_flag': False,
                                       'actual_value': 0.0,
                                       'actual_profile': 0.0,
                                       'last_buy_price': 100000000.0,
                                       }

    def buy(self, symbol, slot):
        if self._slot_position[slot]['qty'] > 0 and self._slot_position[slot]['last_buy_price'] < self.actual_ask_price[symbol]:
            return
        time.sleep(0.04)
        ask_price_fixed = self.actual_ask_price[symbol]
        if not self.qty_rise_flag and self._slot_position[slot]['qty'] <= self.slot[slot]['max_qty']:



            # if self._slot_position[slot]['qty'] == 0:
            #     calc_qty = self.start_buy_qty
            # else:
            #     d = self._slot_position[slot]['income_price']
            #     c = self._slot_position[slot]['qty']
            #     b = ask_price_fixed
            #     s = self.slot['BTCUSDT_SX3']['stop_delta']
            #     calc_qty = ((c*(-d*s+d-b)) / (b*s))
            #     calc_qty = np.max([calc_qty, self.start_buy_qty])
            #     calc_qty = np.min([self.slot[slot]['max_qty'] - self._slot_position[slot]['qty'], calc_qty])

            # print("")
            # print("speed:", self.market_speed_array[-1],
            #       "spread:", self.bid_ask_spread[-1],
            #       "bid qty:", self.best_bid_qty_history[-1],
            #       "ask qty:", self.best_ask_qty_history[-1])

            calc_qty = self.actual_buy_qty
            calc_qty = np.min([self.slot[slot]['max_qty'] - self._slot_position[slot]['qty'], calc_qty])
            calc_qty = round(calc_qty, 4)

            self._slot_position[slot]['symbol'] = symbol

            existed_value = self._slot_position[slot]['income_price'] * self._slot_position[slot]['qty']
            new_value = calc_qty * ask_price_fixed

            self._slot_position[slot]['qty'] += calc_qty  # new qty
            self.monitor_max_qty[slot] = np.max([self.monitor_max_qty[slot], self._slot_position[slot]['qty']])
            self._slot_position[slot]['income_price'] = (existed_value + new_value) / self._slot_position[slot]['qty']
            self._slot_position[slot]['last_buy_price'] = ask_price_fixed
            self._slot_position[slot]['stop_price'] = self._slot_position[slot]['income_price'] * (1 - self.slot[slot]['stop_delta'])
            self._slot_position[slot]['trailer_stop_price'] = self._slot_position[slot]['income_price']  # innen indul és kezdi emelgetni
            self._slot_position[slot]['trailer_minimum_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['trailer_delta'])
            self._slot_position[slot]['take_price'] = self._slot_position[slot]['income_price'] * (1 + self.slot[slot]['take_delta'])
            self._slot_position[slot]['enter_dt'] = datetime.now()
            self._slot_position[slot]['exit_dt'] = self._slot_position[slot]['enter_dt'] + timedelta(seconds=self.slot[slot]['max_time_sec'])
            self._slot_position[slot]['extra_dt'] = self._slot_position[slot]['exit_dt'] + timedelta(seconds=self.slot[slot]['extra_time_sec'])
            self._slot_position[slot]['extra_flag'] = False
            self.total_symbol_position[symbol] += calc_qty

            if self._slot_position[slot]['qty'] < self.profile_activator[0]:
                self.set_profile(1)
            elif self.profile_activator[0] <= self._slot_position[slot]['qty'] < self.profile_activator[1]:
                self.set_profile(2)
            elif self.profile_activator[1] <= self._slot_position[slot]['qty']:
                self.set_profile(3)

            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_long], axis=0), 0)

            self.qty_rise_flag = True
        else:
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)
            # print("")
            # print("Buy", symbol, slot, qty, self.slot_position[slot]['income_price'], self.actual_ask_qty[symbol])

    def stop(self, symbol, slot, message=""):
        time.sleep(0.04)


        # time.sleep(.01)
        # if message in ["Trailer", "Take", "Time"]:
        # print(message)
        income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
        # income_value_short = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price_short']
        exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
        # exit_value_short = self._slot_position[slot]['qty'] * self.actual_ask_price[symbol]
        # print("profit:", exit_value - income_value)
        self.monitor_profit[slot] += (exit_value - income_value)
        # self.monitor_profit_short[slot] += (income_value_short - exit_value_short)
        # print("Close profit:", (exit_value - income_value))
        self.total_symbol_position[symbol] -= self._slot_position[slot]['qty']
        self._slot_position[slot]['symbol'] = ''
        self._slot_position[slot]['qty'] = 0.0
        self._slot_position[slot]['income_price'] = 0.0
        self._slot_position[slot]['last_buy_price'] = 100000000.0
        self._slot_position[slot]['stop_price'] = 0.0
        self._slot_position[slot]['trailer_stop_price'] = 0.0
        self._slot_position[slot]['trailer_minimum_price'] = 0.0
        self._slot_position[slot]['take_price'] = 0.0
        self._slot_position[slot]['enter_dt'] = None
        self._slot_position[slot]['exit_dt'] = None
        self._slot_position[slot]['extra_dt'] = None
        self._slot_position[slot]['extra_flag'] = False
        self.monitor_fee[slot] += round((income_value * 0.025 / 100) + (exit_value * 0.025 / 100), 2)
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_stop], axis=0), 0)
        self.actual_buy_qty = self.start_buy_qty
        self.set_profile(1)
        self.qty_rise_flag = False

    def allowed_rebuy(self):
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_rebuy], axis=0), 0)
        self.actual_buy_qty *= self.buy_multiplier
        self.qty_rise_flag = False

    def get_statistics(self):
        self.status['monitor_stop'] = self.monitor_stop
        self.status['monitor_take'] = self.monitor_take
        self.status['monitor_trailer'] = self.monitor_trailer
        self.status['monitor_re_buy'] = self.monitor_rebuy
        # self.status['monitor_trailer_avg_time'] = self.monitor_trailer_avg_time
        self.status['monitor_time'] = self.monitor_time
        self.status['monitor_profit'] = self.monitor_profit
        # self.status['monitor_profit_short'] = self.monitor_profit_short
        # self.status['monitor_max_loss'] = self.monitor_max_loss
        self.status['monitor_fee'] = self.monitor_fee
        self.status['monitor_max_qty'] = self.monitor_max_qty
        self.status['monitor_min_value'] = self.monitor_min_value
        return self.status

    # @staticmethod
    # def print_statistics(status):
    #     df = pd.DataFrame(status)
    #     df['Deal'] = df['monitor_stop'] + df['monitor_take'] + df['monitor_trailer'] + df['monitor_time']
    #     df['PPT'] = df['monitor_profit'] / df['Deal']
    #
    #     df.rename(columns={'monitor_stop': 'Stop',
    #                        'monitor_take': 'Take',
    #                        'monitor_trailer': 'Trailer',
    #                        'monitor_time': 'Time',
    #                        'monitor_re_buy': 'Re_buy',
    #                        'monitor_profit': 'Profit',
    #                        'monitor_profit_short': 'ProfitSH',
    #                        'monitor_fee': 'Fee',
    #                        }, inplace=True)
    #
    #     print(df)

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

    # async def asyc_websocket_orderbook(self):
    #     i_socket_list = []
    #     for symbol in self._traded_symbols:
    #         # i_socket_list.append(self.get_socket_name(symbol, "trade"))
    #         i_socket_list.append(self.get_socket_name(symbol, "depth20"))
    #         # i_socket_list.append(self.get_socket_name(symbol, "bookticker"))
    #
    #     self.async_client = await AsyncClient.create()
    #     self.bm = BinanceSocketManager(self.async_client)
    #     self.ts = self.bm.multiplex_socket(i_socket_list)
    #
    #     async with self.ts as tscm:
    #         while True:
    #             res = await tscm.recv()
    #             # print(res)
    #             symbol = res['stream'].split('@')[0].upper()
    #             # stype = res['stream'].split('@')[1]
    #             # print(datetime.now(), res)
    #             # {
    #             #   "lastUpdateId": 160,  // Last update ID
    #             #   "bids": [             // Bids to be updated
    #             #     [
    #             #       "0.0024",         // Price level to be updated
    #             #       "10"              // Quantity
    #             #     ]
    #             #   ],
    #             #   "asks": [             // Asks to be updated
    #             #     [
    #             #       "0.0026",         // Price level to be updated
    #             #       "100"             // Quantity
    #             #     ]
    #             #   ]
    #             # }
    #             # if len(self.actual_traded_price[symbol]) >= 3:
    #             #     calc_base_price = np.sum(self.actual_traded_price[symbol][-3:]) / 3
    #             # else:
    #             #     calc_base_price = self.actual_traded_price[symbol][-1]
    #
    #             calc_base_price = np.sum(self.actual_traded_price[symbol][-3:]) / 3
    #             self.base_prices[symbol].append(calc_base_price)
    #             if len(self.base_prices[symbol]) > self.max_depths[symbol]:
    #                 self.base_prices[symbol] = self.base_prices[symbol][1:]
    #
    #             self.orderbooks[symbol].append(res['data'])
    #             self.orderbooks[symbol] = self.orderbooks[symbol][-3:]
    #             # if len(self.orderbooks[symbol]) > self.max_depths[symbol]:
    #
    #             for slot in self._symbol_slot[symbol]:
    #                 if self.total_symbol_position[symbol] == 0:
    #                     await self.trader(symbol, slot, self.orderbooks[symbol], self.base_prices[symbol])

    # async def asyc_websocket_trade(self):
    #     i_socket_list = []
    #     for symbol in self._traded_symbols:
    #         i_socket_list.append(self.get_socket_name(symbol, "trade"))
    #         # i_socket_list.append(self.get_socket_name(symbol, "depth20"))
    #         # i_socket_list.append(self.get_socket_name(symbol, "bookticker"))
    #
    #     self.async_client = await AsyncClient.create()
    #     self.bm = BinanceSocketManager(self.async_client)
    #     self.ts = self.bm.multiplex_socket(i_socket_list)
    #
    #     async with self.ts as tscm:
    #         while True:
    #             res = await tscm.recv()
    #             # print(res)
    #             symbol = res['stream'].split('@')[0].upper()
    #             # stype = res['stream'].split('@')[1]
    #
    #             # {
    #             #   "e": "trade",     // Event type
    #             #   "E": 123456789,   // Event time
    #             #   "s": "BNBBTC",    // Symbol
    #             #   "t": 12345,       // Trade ID
    #             #   "p": "0.001",     // Price
    #             #   "q": "100",       // Quantity
    #             #   "b": 88,          // Buyer order ID
    #             #   "a": 50,          // Seller order ID
    #             #   "T": 123456785,   // Trade time
    #             #   "m": true,        // Is the buyer the market maker?
    #             #   "M": true         // Ignore
    #             # }
    #
    #             self.actual_traded_price[symbol].append(float(res['data']['p']))
    #             self.last_traded_price = float(res['data']['p'])
    #             # if len(self.actual_traded_price) > 3:
    #             self.actual_traded_price[symbol] = self.actual_traded_price[symbol][-3:]

    def trough_detect(self, ts):
        if ts[-1] > ts[-2]:
            return True
        else:
            return False

    async def asyc_websocket_bookticker_detect(self):
        i_socket_list = []
        for symbol in self._traded_symbols:
            i_socket_list.append(self.get_socket_name(symbol, "bookticker"))

        self.async_client = await AsyncClient.create()
        self.bm = BinanceSocketManager(self.async_client)
        self.ts = self.bm.multiplex_socket(i_socket_list)

        # self.market_speed_stamp = datetime.now()

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

                self.actual_bid_price[symbol] = bid
                # self.actual_bid_qty[symbol] = float(res['data']['B'])

                self.best_bid_price_history = np.delete(np.append(self.best_bid_price_history, [bid], axis=0), 0)
                # self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [(np.sum(self.best_bid_qty_history[-10:]) + np.max((bid_qty * -100, -500))) / 11], axis=0), 0)
                # self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [np.max((bid_qty * -100, -500))], axis=0), 0)
                self.best_ask_price_history = np.delete(np.append(self.best_ask_price_history, [ask], axis=0), 0)
                # self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [(np.sum(self.best_ask_qty_history[-10:]) + np.min((ask_qty * 100, 500))) / 11], axis=0), 0)
                # self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [np.min((ask_qty * 100, 500))], axis=0), 0)
                # self.qty_way_history = np.delete(np.append(self.qty_way_history, [np.sum(self.best_ask_qty_history[-250:]) + np.sum(self.best_bid_qty_history[-250:])], axis=0), 0)
                # self.bid_ask_spread = np.delete(np.append(self.bid_ask_spread, [(ask - bid) * 1000], axis=0), 0)
                # self.bid_ask_spread_avg = np.delete(np.append(self.bid_ask_spread_avg, [np.sum(self.bid_ask_spread[-10:]) / 10], axis=0), 0)


                if self.renko_slow_steps <= abs(self.renko_slow_price_history[-1] - ask):
                    self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [ask], axis=0), 0)
                else:
                    self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [self.renko_slow_price_history[-1]], axis=0), 0)

                if self.renko_fast_steps <= abs(self.renko_fast_price_history[-1] - ask):
                    self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [ask], axis=0), 0)
                else:
                    self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [self.renko_fast_price_history[-1]], axis=0), 0)

                self.smoot_slow_price_history = savgol_filter(self.renko_slow_price_history, 300, 1)
                # self.smoot_fast_price_history = savgol_filter(self.renko_fast_price_history, 50, 1)

                # self.smoot_slow_price_history = self.renko_slow_price_history
                self.smoot_fast_price_history = self.renko_fast_price_history

                # c = datetime.now() - self.market_speed_stamp
                # c = c.total_seconds() * 1000 * 10
                # self.market_speed_array = np.delete(np.append(self.market_speed_array, [c], axis=0), 0)
                # self.market_speed_array_avg = np.delete(np.append(self.market_speed_array_avg, [np.sum(self.market_speed_array[-10:]) / 10], axis=0), 0)
                # self.market_speed_stamp = datetime.now()

                ddown = self.smoot_slow_price_history[self.ddown_points] - self.smoot_slow_price_history[-1]
                ddown = ddown > self.ddown_limit


                # if ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):
                # if self.trough_detect(self.smoot_fast_price_history) and ddown.any() and np.max(self.decision_history[-1500:]) == np.min(self.decision_history[-1500:]) == self.decision_neutral\
                #         and not self.slot_in_position('BTCUSDT_SX3'):
                if self.trough_detect(self.smoot_fast_price_history) and ddown.any():
                    # if len(a) > 0 and ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):
                    # if self.best_smoot_price_history[-10] > self.best_smoot_price_history[-11] < self.best_smoot_price_history[-12] and \
                    #     not self.slot_in_position('BTCUSDT_SX3'):

                    # self.decision_history.pop(0)
                    # self.decision_history.append(self.decision_long)

                    symbol = self.slot['BTCUSDT_SX3']['trade_symbol']
                    self.buy(symbol, 'BTCUSDT_SX3')
                else:
                    self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)

    async def asyc_websocket_bookticker_stopper(self):
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
                bid = float(res['data']['b'])
                ask = float(res['data']['a'])

                self.actual_ask_price[symbol] = ask
                # self.actual_ask_qty[symbol] = float(res['data']['A'])

                if self.renko_stop_steps <= abs(self.renko_stop_price_history[-1] - bid):
                    self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [bid], axis=0), 0)
                else:
                    self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [self.renko_stop_price_history[-1]], axis=0), 0)


                self._slot_position[slot]['actual_value'] = self._slot_position[slot]['qty'] * (self.actual_bid_price[symbol] - self._slot_position[slot]['income_price'])
                self.monitor_min_value[slot] = np.min([self.monitor_min_value[slot], self._slot_position[slot]['actual_value']])
                # Stopper



                # selected_price = self.renko_stop_price_history[-1]
                selected_price = self.actual_bid_price[symbol]

                if self._slot_position[slot]['qty'] > 0:

                    # act_trailer_price = synthetic_price
                    act_trailer_price = selected_price * (1 - self.slot[slot]['trailer_delta'])
                    self._slot_position[slot]['trailer_stop_price'] = max(self._slot_position[slot]['trailer_stop_price'], act_trailer_price)

                    if self._slot_position[slot]['take_price'] <= selected_price and self.smoot_slow_price_history[-1] < self.smoot_slow_price_history[-2]:
                        self.stop(symbol, slot, "Take")
                        self.monitor_take[slot] += 1

                        # if self.actual_bid_price[symbol] - self._slot_position[slot]['income_price'] > 2:
                        #     self._slot_position[slot]['stop_price'] = (self._slot_position[slot]['income_price'] + self.actual_bid_price[symbol]) / 5 * 4
                        # else:
                        #     self._slot_position[slot]['stop_price'] = (self._slot_position[slot]['income_price'] + self.actual_bid_price[symbol]) / 2
                        # self._slot_position[slot]['take_price'] = self.actual_bid_price[symbol] * (1 + self.slot[slot]['take_delta'])

                    elif selected_price <= self._slot_position[slot]['stop_price']:
                        # print("    bid", self.actual_bid_price[symbol], "stop   ", self.slot_position[slot]['stop_price'])
                        if self.qty_rise_flag:
                            self.allowed_rebuy()
                            self.monitor_rebuy[slot] += 1

                    elif self._slot_position[slot]['exit_dt'] < datetime.now():
                        if selected_price > self._slot_position[slot]['income_price']:
                            self._slot_position[slot]['exit_dt'] = self._slot_position[slot]['exit_dt'] + timedelta(seconds=self.slot[slot]['extra_time_sec'])

                        # and not self._slot_position[slot]['extra_flag']:
                        # self._slot_position[slot]['extra_flag'] = True
                        # if self.actual_bid_price[symbol] > self._slot_position[slot]['income_price']:
                        #     income_value = self._slot_position[slot]['qty'] * self._slot_position[slot]['income_price']
                        #     exit_value = self._slot_position[slot]['qty'] * self.actual_bid_price[symbol]
                        #     print("Actual profit at reset:", exit_value - income_value)
                        #     self.reset_extra_profit(symbol, slot)
                        else:
                            self.stop(symbol, slot, "Time")
                            self.monitor_time[slot] += 1

                    elif self._slot_position[slot]['trailer_minimum_price'] <= selected_price <= self._slot_position[slot]['trailer_stop_price'] \
                            and self.smoot_slow_price_history[-1] < self.smoot_slow_price_history[-2]:
                        self.stop(symbol, slot, "Trailer")
                        self.monitor_trailer[slot] += 1


if __name__ == '__main__':
    # ntr_nb = ntrade_numba()
    n_tob = TradeOrderBook()
    n_tob.start_threads()
    while True:
        time.sleep(100)

