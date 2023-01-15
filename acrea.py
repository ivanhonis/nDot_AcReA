import multiprocessing
import sys
import time
import pickle
from datetime import datetime, timedelta
# import psutil

import asyncio
from threading import Thread
from multiprocessing import shared_memory, Lock, cpu_count, Process, Array

import numpy as np
from scipy.signal import savgol_filter

from binance import AsyncClient, BinanceSocketManager, Client
# from binance.exceptions import BinanceAPIException
# from binance.enums import *

# from sklearn import linear_model
# from sklearn.preprocessing import PolynomialFeatures
# from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import LinearRegression

np.set_printoptions(threshold=5000)
lock = Lock()


class AcReA:

    def __init__(self, param):
        self.requests_sec_array = param['requests_sec_array']
        self.process = param['process']
        self.cores = param['cores']
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"

        self.symbol = param['base'] + param['quote']
        self.max_invest_quote = float(param['max_invest_quote'])
        # self.free_invest_quote = self.max_invest_quote

        self.minimum_buy_qty_base = float(param['minimum_buy_qty_base'])
        self.start_buy_qty_base = self.minimum_buy_qty_base
        self.actual_buy_qty_base = self.start_buy_qty_base
        self.buy_multiplier = float(param['buy_multiplier'])

        self.trade_profile_limits = np.array(param['trade_profile_limits'])

        self.async_client = None
        self.bm = None
        self.ts = None

        self.async_client2 = None
        self.bm2 = None
        self.ts2 = None

        self.ntick = .5 / 15000
        self.time_period = 20000
        self.slot = {'stop_delta': 0.0,
                     'trailer_delta': 0.0,
                     'take_delta': 0.0,
                     'max_time_sec': 0,
                     'extra_time_sec': 0,
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

        self.slot_position = {'qty': 0.0,
                              'income_price': 0.0,
                              'free_invest_quote': self.max_invest_quote,
                              'stop_price': 0.0,
                              'trailer_stop_price': 0.0,
                              'trailer_minimum_price': 0.0,
                              'take_price': 0.0,
                              'enter_dt': None,
                              'exit_dt': None,
                              'extra_dt': None,
                              'extra_flag': False,
                              'actual_value': 0.0,
                              'actual_profile': 0.0,
                              'last_buy_price': 100000000.0,
                              }

        self.slot_position_index = {'qty': 0,
                                    'income_price': 1,
                                    'free_invest_quote': 2,
                                    'stop_price': 3,
                                    'trailer_stop_price': 4,
                                    'trailer_minimum_price': 5,
                                    'take_price': 6,
                                    'actual_value': 7,
                                    'actual_profile': 8,
                                    'last_buy_price': 9,
                                    }

        # két esetben direkt címzem (4,7) a többi esetben set_slot_position_array használom
        # két esetben számít a sebesség

        ###################
        #  shared memory  #
        ###################

        self.slot_position_array = param['slot_position_array']

        # self.slot_position_array = np.array([0.0] * 10, dtype=np.float32)

        # utolsó simert orderbook legjob datai
        # ez kell a buy és a stop hoz
        self.actual_bid_price = 0.0
        self.actual_ask_price = 0.0
        # self.actual_bid_qty = 0.0
        # self.actual_ask_qty = 0.0

        self.renko_slow_steps = 0.0
        self.renko_fast_steps = 0.0
        self.renko_stop_steps = 0.0
        self.ddown = np.array([False] * self.time_period, dtype=bool)
        self.ddown_limit = 0.0
        self.ddown_points = np.array([])

        self.qty_rise_flag = False

        self.best_bid_price_history = np.array([0.0] * self.time_period)
        self.best_ask_price_history = np.array([0.0] * self.time_period)
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
        # self.traded_price_history = [0.0] * self.time_period

        self.decision_neutral = 0
        self.decision_long = 500
        self.decision_stop = -500
        self.decision_buy_again = 250
        self.decision_history = np.array([self.decision_neutral] * self.time_period)

        self.transfer = np.empty((11, self.time_period))
        self.transfer_status = np.empty(10)

        self.monitor_max_qty = 0.0
        self.monitor_min_value = 0.0
        self.status_array_index = {'stop': 0,
                                   'buy_again': 1,
                                   'take': 2,
                                   'trailer': 3,
                                   'time': 4,
                                   'profit': 5,
                                   'turnover': 6,
                                   'max_qty': 7,
                                   'min_value': 8
                                   }

        ###################
        #  shared memory  #
        ###################

        self.status_array = param['status_array']

        self.actual_profile = 1
        self.set_profile(1, direct=True)

        self.start_threads()

    def set_slot_position_array(self):
        for key in self.slot_position:
            if key in self.slot_position_index:
                self.slot_position_array[int(self.slot_position_index[key])] = float(self.slot_position[key])

    def status(self, name, value, static=False):
        if static:
            self.status_array[self.status_array_index[name]] = value
        else:
            self.status_array[self.status_array_index[name]] += value

    def set_profile(self, p, direct=False):
        if p != self.actual_profile < p or direct:
            self.slot['stop_delta'] = self.trading_profile[p]['stop_delta']
            self.slot['trailer_delta'] = self.trading_profile[p]['trailer_delta']
            self.slot['take_delta'] = self.trading_profile[p]['take_delta']
            self.slot['max_time_sec'] = self.trading_profile[p]['max_time_sec']
            self.slot['extra_time_sec'] = self.trading_profile[p]['extra_time_sec']

            self.renko_slow_steps = self.trading_profile[p]['renko_slow_steps']
            self.renko_fast_steps = self.trading_profile[p]['renko_fast_steps']
            self.renko_stop_steps = self.trading_profile[p]['renko_stop_steps']
            self.ddown_limit = self.trading_profile[p]['ddown_limit']
            self.ddown_points = np.arange(self.trading_profile[p]['ddown_depth'], -1, 20)
            self.actual_profile = p
            self.slot_position['actual_profile'] = p * 1.0
            self.set_slot_position_array()

    def start_threads(self):
        task1 = Thread(target=self.bookticker_stopper, args=[])
        # task2 = Thread(target=self.trade_thr, args=[])
        # task3 = Thread(target=self.orderbook_thr, args=[])
        task4 = Thread(target=self.bookticker_detect, args=[])
        # task5 = Thread(target=self.deal_hunter_thr, args=[])
        # task6 = Thread(target=self.data_transfer, args=[])

        task1.start()
        time.sleep(2)

        # task2.start()
        # time.sleep(1)
        task4.start()
        task1.join()
        task4.join()
        # time.sleep(3)
        # task6.start()

    def data_transfer(self):
        while True:

            # self.transfer[0] = np.array(self.best_bid_price_history)
            # self.transfer[1] = np.array(self.best_ask_price_history)
            # self.transfer[2] = np.array(self.smoot_slow_price_history)
            # self.transfer[3] = np.array(self.decision_history)
            # # self.transfer[4] = np.array(self.market_speed_array_avg)
            # # self.transfer[5] = np.array(self.market_speed_array)
            # # self.transfer[6] = np.array(self.bid_ask_spread_avg)
            # # self.transfer[7] = np.array(self.bid_ask_spread)
            # # self.transfer[8] = np.array(self.best_ask_qty_history)
            # # self.transfer[9] = np.array(self.best_bid_qty_history)
            # self.transfer[10] = np.array(self.smoot_fast_price_history)
            # np.save("transfer_timeseries.npy", self.transfer)
            #
            # f = open("transfer_position.pkl", "wb")
            # pickle.dump(self.slot_position, f)
            # f.close()
            #
            # f = open("transfer_statistics.pkl", "wb")
            # pickle.dump(self.status_array, f)
            # f.close()

            print(self.slot_position)
            print(self.slot_position_array[:])
            print(self.status_array[:])
            time.sleep(7)

    # def moving_average(self, x, w):
    #     iret = np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])
    #     return iret

    def bookticker_stopper(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_websocket_bookticker_stopper())
        loop.close()

    def bookticker_detect(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_websocket_bookticker_detect())
        loop.close()

    def set_request(self):
        t = datetime.now().time()
        seconds = (t.hour * 60 + t.minute) * 60 + t.second
        self.requests_sec_array[seconds] += 1

    def buy(self):
        if self.slot_position['qty'] > 0 and self.slot_position['last_buy_price'] < self.actual_ask_price:
            return
        time.sleep(0.04)

        ask_price_fixed = self.actual_ask_price
        max_buy_base = round(self.slot_position['free_invest_quote'] / ask_price_fixed, 4)
        calc_qty = round(self.actual_buy_qty_base, 4)
        calc_qty = np.min([max_buy_base, calc_qty])

        if not self.qty_rise_flag and calc_qty >= self.minimum_buy_qty_base:

            existed_value = self.slot_position['income_price'] * self.slot_position['qty']
            new_value = calc_qty * ask_price_fixed

            self.slot_position['qty'] += calc_qty  # new qty
            self.slot_position['free_invest_quote'] -= new_value
            self.monitor_max_qty = np.max([self.monitor_max_qty, self.slot_position['qty']])
            self.status('max_qty', self.monitor_max_qty, static=True)
            self.status('turnover', calc_qty * ask_price_fixed)
            self.slot_position['income_price'] = (existed_value + new_value) / self.slot_position['qty']
            self.slot_position['last_buy_price'] = ask_price_fixed
            self.slot_position['stop_price'] = self.slot_position['income_price'] * (1 - self.slot['stop_delta'])
            self.slot_position['trailer_stop_price'] = self.slot_position['income_price']  # innen indul és kezdi emelgetni
            self.slot_position['trailer_minimum_price'] = self.slot_position['income_price'] * (1 + self.slot['trailer_delta'])
            self.slot_position['take_price'] = self.slot_position['income_price'] * (1 + self.slot['take_delta'])
            self.slot_position['enter_dt'] = datetime.now()
            self.slot_position['exit_dt'] = self.slot_position['enter_dt'] + timedelta(seconds=self.slot['max_time_sec'])
            self.slot_position['extra_dt'] = self.slot_position['exit_dt'] + timedelta(seconds=self.slot['extra_time_sec'])
            self.slot_position['extra_flag'] = False

            # Risk management
            position_value = self.slot_position['qty'] * self.actual_bid_price
            total_exit_value = self.slot_position['free_invest_quote'] + position_value
            if self.trade_profile_limits[0] <= position_value / total_exit_value < self.trade_profile_limits[1]:
                self.set_profile(2)
            elif self.trade_profile_limits[1] <= position_value / total_exit_value:
                self.set_profile(3)

            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_long], axis=0), 0)

            self.set_slot_position_array()

            self.set_request()

            self.qty_rise_flag = True
        else:
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)
            # print("")
            # print("Buy", symbol, slot, qty, self.slot_position['income_price'], self.actual_ask_qty[symbol])

    def stop(self, message=""):
        time.sleep(0.04)

        # time.sleep(.01)
        # if message in ["Trailer", "Take", "Time"]:
        # print(message)
        income_value = self.slot_position['qty'] * self.slot_position['income_price']
        # income_value_short = self._slot_position['qty'] * self._slot_position['income_price_short']
        exit_value = self.slot_position['qty'] * self.actual_bid_price
        # exit_value_short = self._slot_position['qty'] * self.actual_ask_price[symbol]
        # print("profit:", exit_value - income_value)
        self.status('profit', exit_value - income_value)
        # self.monitor_profit_short += (income_value_short - exit_value_short)
        # print("Close profit:", (exit_value - income_value))
        self.slot_position['free_invest_quote'] += exit_value
        self.slot_position['qty'] = 0.0
        self.slot_position['income_price'] = 0.0
        self.slot_position['last_buy_price'] = 100000000.0
        self.slot_position['stop_price'] = 0.0
        self.slot_position['trailer_stop_price'] = 0.0
        self.slot_position['trailer_minimum_price'] = 0.0
        self.slot_position['take_price'] = 0.0
        self.slot_position['enter_dt'] = None
        self.slot_position['exit_dt'] = None
        self.slot_position['extra_dt'] = None
        self.slot_position['extra_flag'] = False
        # self.monitor_fee += round((income_value * 0.025 / 100) + (exit_value * 0.025 / 100), 2)
        self.status('turnover', exit_value)
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_stop], axis=0), 0)
        self.actual_buy_qty_base = self.start_buy_qty_base
        self.set_profile(1, direct=True)
        self.set_slot_position_array()
        self.set_request()
        self.qty_rise_flag = False

    def allowed_buy_again(self):
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_buy_again], axis=0), 0)
        self.actual_buy_qty_base *= self.buy_multiplier
        self.qty_rise_flag = False

    def slot_in_position(self):
        if self.slot_position['qty'] > 0:
            return True
        elif self.slot_position['qty'] == 0:
            return False
        else:
            print("Minusz pozíció!!!!!")
            # TODO ezt a hibát kezelni kell
            sys.exit()

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

    @staticmethod
    def trough_detect(ts):
        if ts[-1] > ts[-2]:
            return True
        else:
            return False

    async def add_best_bid_price_history(self, bid):
        self.best_bid_price_history = np.delete(np.append(self.best_bid_price_history, [bid], axis=0), 0)

    async def add_best_ask_price_history(self, ask):
        self.best_ask_price_history = np.delete(np.append(self.best_ask_price_history, [ask], axis=0), 0)

    async def add_renko_slow_price_history_savgol_ddown(self, ask):
        if self.renko_slow_steps <= abs(self.renko_slow_price_history[-1] - ask):
            self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [ask], axis=0), 0)
        else:
            self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [self.renko_slow_price_history[-1]], axis=0), 0)

        self.smoot_slow_price_history = savgol_filter(self.renko_slow_price_history, 300, 1)

        ddown = self.smoot_slow_price_history[self.ddown_points] - self.smoot_slow_price_history[-1]
        self.ddown = ddown > self.ddown_limit

    async def add_renko_fast_price_history(self, ask):
        if self.renko_fast_steps <= abs(self.renko_fast_price_history[-1] - ask):
            self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [ask], axis=0), 0)
        else:
            self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [self.renko_fast_price_history[-1]], axis=0), 0)

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
                #
                self.actual_bid_price = bid
                # self.actual_bid_qty[symbol] = float(res['data']['B'])

                # dt1 = datetime.now()
                # self.best_bid_price_history = np.delete(np.append(self.best_bid_price_history, [bid], axis=0), 0)

                # # self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [(np.sum(self.best_bid_qty_history[-10:])
                # + np.max((bid_qty * -100, -500))) / 11], axis=0), 0)

                # # self.best_bid_qty_history = np.delete(np.append(self.best_bid_qty_history, [np.max((bid_qty * -100, -500))], axis=0), 0)
                # self.best_ask_price_history = np.delete(np.append(self.best_ask_price_history, [ask], axis=0), 0)

                # # self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [(np.sum(self.best_ask_qty_history[-10:])
                # + np.min((ask_qty * 100, 500))) / 11], axis=0), 0)

                # # self.best_ask_qty_history = np.delete(np.append(self.best_ask_qty_history, [np.min((ask_qty * 100, 500))], axis=0), 0)

                # # self.qty_way_history = np.delete(np.append(self.qty_way_history, [np.sum(self.best_ask_qty_history[-250:])
                # + np.sum(self.best_bid_qty_history[-250:])], axis=0), 0)

                # # self.bid_ask_spread = np.delete(np.append(self.bid_ask_spread, [(ask - bid) * 1000], axis=0), 0)
                # # self.bid_ask_spread_avg = np.delete(np.append(self.bid_ask_spread_avg, [np.sum(self.bid_ask_spread[-10:]) / 10], axis=0), 0)
                #
                # if self.renko_slow_steps <= abs(self.renko_slow_price_history[-1] - ask):
                #     self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [ask], axis=0), 0)
                # else:
                #     self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [self.renko_slow_price_history[-1]], axis=0), 0)
                #
                # if self.renko_fast_steps <= abs(self.renko_fast_price_history[-1] - ask):
                #     self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [ask], axis=0), 0)
                # else:
                #     self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [self.renko_fast_price_history[-1]], axis=0), 0)
                # print((datetime.now() - dt1) * 10000)

                # dt1 = datetime.now()
                asyncio.gather(self.add_best_ask_price_history(ask),
                               self.add_best_bid_price_history(bid),
                               self.add_renko_slow_price_history_savgol_ddown(ask),
                               self.add_renko_fast_price_history(ask))
                # print((datetime.now() - dt1) * 10000 )

                # self.smoot_slow_price_history = savgol_filter(self.renko_slow_price_history, 300, 1)
                # print((datetime.now() - dt1) * 10000)
                # self.smoot_fast_price_history = savgol_filter(self.renko_fast_price_history, 50, 1)

                # self.smoot_slow_price_history = self.renko_slow_price_history
                # self.smoot_fast_price_history = self.renko_fast_price_history

                # ddown = self.smoot_slow_price_history[self.ddown_points] - self.smoot_slow_price_history[-1]
                # ddown = ddown > self.ddown_limit

                if self.trough_detect(self.renko_fast_price_history) and self.ddown.any():
                    # if len(a) > 0 and ddown.any() and not self.slot_in_position('BTCUSDT_SX3'):
                    # if self.best_smoot_price_history[-10] > self.best_smoot_price_history[-11] < self.best_smoot_price_history[-12] and \
                    #     not self.slot_in_position('BTCUSDT_SX3'):

                    # self.decision_history.pop(0)
                    # self.decision_history.append(self.decision_long)

                    self.buy()
                else:
                    self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)

    async def async_websocket_bookticker_stopper(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]
        # for symbol in self._traded_symbols:

        self.async_client2 = await AsyncClient.create()
        self.bm2 = BinanceSocketManager(self.async_client2)
        self.ts2 = self.bm2.multiplex_socket(i_socket_list)

        async with self.ts2 as tscm:
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
                # bid = float(res['data']['b'])
                ask = float(res['data']['a'])

                self.actual_ask_price = ask
                # self.actual_ask_qty[symbol] = float(res['data']['A'])

                # if self.renko_stop_steps <= abs(self.renko_stop_price_history[-1] - bid):
                #     self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [bid], axis=0), 0)
                # else:
                #     self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [self.renko_stop_price_history[-1]], axis=0), 0)

                self.slot_position['actual_value'] = self.slot_position['qty'] * (self.actual_bid_price - self.slot_position['income_price'])
                self.slot_position_array[7] = self.slot_position['actual_value']

                # print(self.slot_position_array)
                self.monitor_min_value = np.min([self.monitor_min_value, self.slot_position['actual_value']])
                self.status('min_value', self.monitor_min_value, static=True)
                # Stopper

                # selected_price = self.renko_stop_price_history[-1]
                selected_price = self.actual_bid_price

                if self.slot_position['qty'] > 0:

                    # act_trailer_price = synthetic_price
                    act_trailer_price = selected_price * (1 - self.slot['trailer_delta'])
                    self.slot_position['trailer_stop_price'] = max(self.slot_position['trailer_stop_price'], act_trailer_price)
                    self.slot_position_array[4] = self.slot_position['trailer_stop_price']

                    if self.slot_position['take_price'] <= selected_price and self.smoot_slow_price_history[-1] < self.smoot_slow_price_history[-2]:
                        self.stop("Take")
                        self.status('take', 1)

                        # if self.actual_bid_price[symbol] - self._slot_position['income_price'] > 2:
                        #     self._slot_position['stop_price'] = (self._slot_position['income_price'] + self.actual_bid_price[symbol]) / 5 * 4
                        # else:
                        #     self._slot_position['stop_price'] = (self._slot_position['income_price'] + self.actual_bid_price[symbol]) / 2
                        # self._slot_position['take_price'] = self.actual_bid_price[symbol] * (1 + self.slot['take_delta'])

                    elif selected_price <= self.slot_position['stop_price']:
                        # print("    bid", self.actual_bid_price[symbol], "stop   ", self.slot_position['stop_price'])
                        if self.qty_rise_flag:
                            self.allowed_buy_again()
                            self.status('buy_again', 1)

                    elif self.slot_position['exit_dt'] < datetime.now():
                        if selected_price > self.slot_position['income_price']:
                            self.slot_position['exit_dt'] = self.slot_position['exit_dt'] + timedelta(seconds=self.slot['extra_time_sec'])

                        # and not self._slot_position['extra_flag']:
                        # self._slot_position['extra_flag'] = True
                        # if self.actual_bid_price[symbol] > self._slot_position['income_price']:
                        #     income_value = self._slot_position['qty'] * self._slot_position['income_price']
                        #     exit_value = self._slot_position['qty'] * self.actual_bid_price[symbol]
                        #     print("Actual profit at reset:", exit_value - income_value)
                        #     self.reset_extra_profit(symbol, slot)
                        else:
                            self.stop("Time")
                            self.status('time', 1)

                    elif self.slot_position['trailer_minimum_price'] <= selected_price <= self.slot_position['trailer_stop_price'] \
                            and self.smoot_slow_price_history[-1] < self.smoot_slow_price_history[-2]:
                        self.stop("Trailer")
                        self.status('trailer', 1)


class Monitor:

    def __init__(self, param):
        self.status_array = param['status_array']
        self.slot_position_array = param['slot_position_array']
        self.requests_sec_array = param['requests_sec_array']
        self.process = param['process']
        self.cores = param['cores']
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        self.data_manager()

    @staticmethod
    def moving_average(x, w):
        return np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])

    def data_manager(self):
        while True:
            slot_position_dict = {'qty': round(self.slot_position_array[0], 8),
                                  'income_price': round(self.slot_position_array[1], 8),
                                  'free_invest_quote': round(self.slot_position_array[2], 8),
                                  'stop_price': round(self.slot_position_array[3], 8),
                                  'trailer_stop_price': round(self.slot_position_array[4], 8),
                                  'trailer_minimum_price': round(self.slot_position_array[5], 8),
                                  'take_price': round(self.slot_position_array[6], 8),
                                  'actual_value_quote': round(self.slot_position_array[7], 8),
                                  'actual_profile': round(self.slot_position_array[8], 8),
                                  'last_buy_price': round(self.slot_position_array[9], 8),
                                  }

            status_array_dict = {'stop': int(self.status_array[0]),
                                 'buy_again': int(self.status_array[1]),
                                 'take': int(self.status_array[2]),
                                 'trailer': int(self.status_array[3]),
                                 'time': int(self.status_array[4]),
                                 'profit': round(self.status_array[5], 8),
                                 'turnover': round(self.status_array[6], 8),
                                 'max_qty': round(self.status_array[7], 8),
                                 'min_value_qoute': round(self.status_array[8], 8)
                                 }

            print(slot_position_dict)
            print(status_array_dict)

            print("request / minute (limit 1200):", np.max(self.moving_average(self.requests_sec_array, 60)) * 60 )
            print("request / 10 sec (limit 50):", np.max(self.moving_average(self.requests_sec_array, 10)) * 10)
            print("24 hours limit(160 000):", np.sum(self.requests_sec_array))

            time.sleep(3)


if __name__ == '__main__':
    cores = cpu_count()
    running_processes = []

    ###################
    #  shared memory  #
    ###################

    status_array = Array('f', [0.0] * 9)
    slot_position_array = Array('f', [0.0] * 10)
    requests_sec_array = Array('i', [0] * 86400)

    n_acrea = AcReA
    n_monitor = Monitor

    params = {'cores': cores,
              'process': 1,
              'base': "BTC",
              'quote': "USDT",
              'max_invest_quote': 750 * 3,
              'minimum_buy_qty_base': 0.0002,
              'buy_multiplier': 1.1,
              'trade_profile_limits': [.1, .3],
              'status_array': status_array,
              'slot_position_array': slot_position_array,
              'requests_sec_array': requests_sec_array,
              }

    process1 = Process(target=n_acrea, args=(params,))
    process1.start()

    params2 = {'cores': cores,
               'process': 2,
               'base': "BTC",
               'quote': "USDT",
               'max_invest_quote': 750 * 3,
               'minimum_buy_qty_base': 0.0002,
               'buy_multiplier': 1.1,
               'trade_profile_limits': [.1, .3],
               'status_array': status_array,
               'slot_position_array': slot_position_array,
               'requests_sec_array': requests_sec_array,
               }

    process2 = Process(target=n_monitor, args=(params2,))
    process2.start()

    process1.join()
    process2.join()

    while True:
        print("1")
        time.sleep(1)
