import sys
import time
import os
# import pickle
from datetime import datetime, timedelta
# import psutil

# from numba import int32, types, typed
# from numba.experimental import jitclass
# from numba import njit, objmode, void
# import numba
# import psutil

import asyncio
from threading import Thread
from multiprocessing import cpu_count, Process, Array
# import subprocess

import numpy as np
# from scipy.signal import savgol_filter
from scipy.ndimage import uniform_filter1d

from binance import AsyncClient, BinanceSocketManager, Client
# from binance.exceptions import BinanceAPIException
from binance.enums import *

# from sklearn import linear_model
# from sklearn.preprocessing import PolynomialFeatures
# from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import LinearRegression
np.set_printoptions(threshold=5000)


class SocketActionLimit:
    # market sebesség mérő

    def __init__(self, t_period, extra_time):
        self.time_period = t_period
        self.extra_time = extra_time
        self.socket_action = {}
        self.trade_allowe_timestamp = 0
        self.trade_ban = 60 * 6  # sec
        self.speed_limit = 220  # socket action per sec
        self.socket_counter = 0

    def is_ready_to_start(self):
        if self.socket_counter > self.time_period + self.extra_time:
            self.socket_counter = self.time_period + self.extra_time + 10
            # ez azért kell hogy ne számoljon a végtelenig +10 re vissza ugratja és onnan növelgeti
            return True
        else:
            return False

    def set_action(self, timestamp):
        self.socket_counter += 1
        if timestamp in self.socket_action:
            self.socket_action[timestamp] += 1
        else:
            self.socket_action[timestamp] = 1

        if self.socket_action[timestamp] > self.speed_limit:
            self.trade_allowe_timestamp = timestamp + self.trade_ban

        if timestamp - 3 in self.socket_action:
            del self.socket_action[timestamp - 3]

    def set_socket_counter(self, no):
        self.socket_counter = no

    def get_last_summa(self, timestamp, shift=0):
        if (timestamp - 1 + shift) in self.socket_action:
            return self.socket_action[timestamp - 1 + shift]
        else:
            return 0

    def is_trade_allow(self, timestamp):
        if timestamp > self.trade_allowe_timestamp:
            return True
        else:
            return False


class BinanceActionLimit:
    # TODO meg kell csinálni hogy napon átnyúló esetben is jól számolja a limiteket

    def __init__(self):
        self.request_reg = np.array([0] * 86400, dtype=np.int32)
        self.request_reg_yesterday = np.array([0] * 86400, dtype=np.int32)
        self.order_reg = np.array([0] * 86400, dtype=np.int32)
        self.order_reg_yesterday = np.array([0] * 86400, dtype=np.int32)
        self.request_limit_sec = 60
        self.request_limit_action = 1200 - 1
        self.order_limit_1_sec = 10
        self.order_limit_1_action = 50 - 1
        self.order_limit_2_action = 160000 - 1
        self.count_block_action = 0
        # a limitet csak vételkor ellenőrzöm, eladni minden képen lehet, azért a lmitáló 1-el kevesebb
        # vételt enged, hogy egy eladásra mindenképen maradjon lehetőség

    @staticmethod
    def sma(a, p):
        m = np.cumsum(a) / p
        m[p:] = m[p:] - m[:-p]  # Odd behavior of -= in Numba
        # m[:p - 1] = np.nan
        m = m[p:]
        return m

    def reg_request_action(self, sec):
        self.request_reg[sec] += 1

    def get_max_request(self):
        return int(np.max(self.sma(self.request_reg, self.request_limit_sec)) * self.request_limit_sec)

    def get_max_order(self):
        return int(np.max(self.sma(self.order_reg, self.order_limit_1_sec)) * self.order_limit_1_sec)

    def get_sum_orders24(self, sec):
        return np.sum(self.order_reg[0:sec + 1]) + np.sum(self.order_reg_yesterday[86400 - (86400 - sec):-1])

    def reg_order_action(self, sec):
        self.order_reg[sec] += 1
        self.request_reg[sec] += 1

    def shift_day(self):
        self.order_reg_yesterday = self.order_reg.copy()
        self.order_reg = np.array([0] * 86400, dtype=np.int32)

        self.request_reg_yesterday = self.request_reg.copy()
        self.request_reg = np.array([0] * 86400, dtype=np.int32)

    def get_blocked_actions(self):
        return self.count_block_action

    def is_action_limit_ok(self, sec):
        back_request_limit_sec = sec - self.request_limit_sec
        back_order_limit_1_sec = sec - self.order_limit_1_sec
        # biztonság kedvéért az aktuális másodpercet is beleszámolom
        # ezért ez több mint 10 de kevesebb mint 11 másodperc
        if back_request_limit_sec < 0:
            requests = np.sum(self.request_reg[0:sec + 1]) + np.sum(self.request_reg[back_request_limit_sec:])
        else:
            requests = np.sum(self.request_reg[back_request_limit_sec:sec + 1])

        if back_order_limit_1_sec < 0:
            orders_1 = np.sum(self.order_reg[0:sec + 1]) + np.sum(self.order_reg[back_order_limit_1_sec:])
        else:
            orders_1 = np.sum(self.order_reg[back_order_limit_1_sec:sec + 1])

        orders_2 = np.sum(self.order_reg[0:sec + 1]) + np.sum(self.order_reg_yesterday[86400-(86400-sec):-1])

        if requests < self.request_limit_action and orders_1 < self.order_limit_1_action and orders_2 < self.order_limit_2_action:
            return True
        else:
            self.count_block_action += 1
            return False


class TradeRegister:
    def __init__(self):
        self.price_arr = np.array([], dtype=np.float32)
        self.qty_arr = np.array([], dtype=np.float32)

    def add_trade(self, qty, price):
        self.price_arr = np.append(self.price_arr, [price], axis=0)
        self.qty_arr = np.append(self.qty_arr, [qty], axis=0)

    def get_qty(self, price, spread=0):
        index = np.where(self.price_arr < price - spread)[0]
        income_value = np.sum(self.price_arr[index] * self.qty_arr[index])
        return np.sum(self.qty_arr[index]), income_value

    def remove_qty(self, price, spread=0):
        index = np.where(self.price_arr < price - spread)[0]
        self.price_arr = np.delete(self.price_arr, index)
        self.qty_arr = np.delete(self.qty_arr, index)

    def get_open_position_income_price(self):
        sq = np.sum(self.qty_arr)
        v = 0
        if sq > 0:
            v = np.sum(self.price_arr * self.qty_arr) / sq
        return v

    def get_open_position_value(self):
        v = np.sum(self.price_arr * self.qty_arr)
        return v

    def get_total_qty(self):
        v = np.sum(self.qty_arr)
        return float(v)


class GridAcReA:
    def __init__(self, param):
        self.stop_flag = param['stop_flag']
        self.back_length = param['back_length']
        self.round_block = param['round_block']
        self.trade_in_progres = True
        self.threads_in_progress = True

        self.sh_status_array = param['status_array']
        self.simulation = param['simulation']
        self.reg_trading_mode()

        self.time_period = param['time_period']
        self.sal = SocketActionLimit(t_period=5000, extra_time=0)
        self.tr = TradeRegister()
        self.max_socket_action = 0
        self.max_socket_action_dt = datetime.now()

        self.deposit_quote = param['deposit_quote']
        self.server_start_dt = datetime.now()
        time.sleep(1)  # azért, hogy elkerüljem a nullával való osztátst, mert a profitot eltelt másodpercre számolom

        self.sh_trade_time = param['trade_time']  # az utolsó 100 kötés sebességét eltáromol
        self.trade_time = np.array([0] * 100, dtype=np.int32)

        self.bal = BinanceActionLimit()
        self.binance_action_limit = param['binance_action_limit']

        self.commission_alert = 0  # az utolsó ügyletnél felszámításra került comisson akkor annak értéke ide kerül,

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

        # self.trade_profile_limits = np.array(param['trade_profile_limits'])  # a befektetésével (beáramlásával) állítja a profil szintek (%)

        self.api_key = "sNtEg0vnKFm09xKf8v9VJWIYspFFouJN5vJO9bSgUSAU8eoAUa5OaMuJYLLsuswr"
        self.api_secret = "Hhu6MPTOdEKBjZyooPr1JxbgiLME3VFJdymqJrtytrMywKatP08Y5G1Sb9ZuJv4S"
        self.bx_client = Client(self.api_key, self.api_secret)

        self.async_client_detect = None
        self.bm_detect = None
        self.ts_detect = None

        self.async_client_stopper = None
        self.bm_stopper = None
        self.ts_stopper = None

        self.slot_position = {'qty': 0.0,
                              'income_price': 0.0,
                              'free_invest_quote': self.max_invest_quote,
                              'actual_value': 0.0,
                              'last_buy_price': 100000000.0,
                              'exit_value': 0.0,
                              }

        self.slot_position_index = {'qty': 0,
                                    'income_price': 1,
                                    'free_invest_quote': 2,
                                    # 'stop_price': 3,
                                    # 'trailer_stop_price': 4,
                                    # 'trailer_minimum_price': 5,
                                    # 'take_price': 6,
                                    'actual_value': 7,
                                    # 'actual_profile': 8,
                                    'last_buy_price': 9,
                                    'exit_value': 10,
                                    }

        # két esetben direkt címzem (4,7) a többi esetben set_slot_position_array használom
        # két esetben számít a sebesség

        self.sh_slot_position_array = param['slot_position_array']

        # self.slot_position_array = np.array([0.0] * 10, dtype=np.float32)

        # utolsó ismert orderbook legjobb adatai
        # ez kell a buy és a stop hoz
        self.actual_bid_price = 0.0
        self.actual_ask_price = 0.0
        # self.actual_bid_qty = 0.0
        # self.actual_ask_qty = 0.0

        self.best_bid_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.best_ask_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.uniform_bid_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.uniform_ask_price_history = np.array([0.0] * self.time_period, dtype=np.float32)

        self.sh_best_bid_price_history = param['best_bid_price_history']
        self.sh_best_ask_price_history = param['best_ask_price_history']
        self.sh_uniform_bid_price_history = param['uniform_bid_price_history']
        self.sh_uniform_ask_price_history = param['uniform_ask_price_history']

        self.decision_neutral = 0
        self.decision_long = 500
        self.decision_stop = -500

        self.decision_history = np.array([0] * self.time_period, dtype=np.int32)
        self.sh_decision_history = param['decision_history']

        self.transfer = np.empty((11, self.time_period))
        self.transfer_status = np.empty(10)

        self.monitor_max_qty = 0.0
        self.monitor_min_value = 0.0
        self.status_array_index = {
                                   # 'stop': 0,
                                   # 'buy_again': 1,
                                   # 'take': 2,
                                   # 'trailer': 3,
                                   # 'time': 4,
                                   'profit': 5,
                                   'turnover': 6,
                                   'max_qty': 7,
                                   'min_value': 8,
                                   }

        # self.actual_profile = 1
        # self.set_profile(1, direct=True)

        self.start_threads()

    def reg_trading_mode(self):
        ipath = r"./real_trading.txt"
        if os.path.exists(ipath):
            os.remove(ipath)

        ipath = r"./simulation.txt"
        if os.path.exists(ipath):
            os.remove(ipath)

        if self.simulation:
            with open("./simulation.txt", "a") as f:
                print("It is just a simulation!", file=f)
            print("It is just a simulation!")
        else:
            with open("./real_trading.txt", "a") as f:
                print("AcReA in REAL TRADING mode!", file=f)
            print("AcReA in REAL TRADING mode!")

        # egyéb státusz elemk regisztrálása
        if self.simulation:
            self.sh_status_array[10] = 1
        else:
            self.sh_status_array[10] = 0

    # def min_max_bid(self, x, y):
    #     return (np.max(self.best_bid_price_history) - np.min(self.best_bid_price_history)) / y * x
    #
    # def min_max_ask(self, x, y):
    #     return (np.max(self.best_ask_price_history) - np.min(self.best_ask_price_history)) / y * x
    #
    # def max_bid(self):
    #     return np.max(self.best_bid_price_history)
    #
    # def max_ask(self):
    #     return np.max(self.best_ask_price_history)

    @property
    def time_delta(self):
        return int((datetime.now() - self.server_start_dt).total_seconds())

    def set_slot_position_array(self):
        for key in self.slot_position:
            if key in self.slot_position_index:
                self.sh_slot_position_array[int(self.slot_position_index[key])] = float(self.slot_position[key])

    def status(self, name, value, static=False):
        if static:
            self.sh_status_array[self.status_array_index[name]] = value
        else:
            self.sh_status_array[self.status_array_index[name]] += value

        # egyéb státusz elemk regisztrálása
        if self.simulation:
            self.sh_status_array[10] = 1
        else:
            self.sh_status_array[10] = 0

        # profitot számolja ki
        self.sh_status_array[9] = round(self.sh_status_array[5] / self.time_delta * (24*60*60*365) / self.deposit_quote, 4)

    def start_threads(self):
        task1 = Thread(target=self.bookticker_stopper, args=[])
        task2 = Thread(target=self.bookticker_detect, args=[])
        task3 = Thread(target=self.data_transfer, args=[])

        task1.start()
        time.sleep(2)

        task2.start()
        time.sleep(3)

        task3.start()

        task1.join()
        task2.join()
        task2.join()

    def data_transfer(self):
        while self.threads_in_progress:
            self.sh_best_bid_price_history[:] = self.best_bid_price_history[:]
            self.sh_best_ask_price_history[:] = self.best_ask_price_history[:]
            self.sh_uniform_bid_price_history[:] = self.uniform_bid_price_history[:]
            self.sh_uniform_ask_price_history[:] = self.uniform_ask_price_history[:]
            self.sh_decision_history[:] = self.decision_history[:]
            self.sh_trade_time[:] = self.trade_time[:]
            self.set_slot_position_array()

            # dti = int(datetime.now().timestamp())
            # a1 = self.sal.get_last_summa(dti)
            # a2 = self.sal.get_last_summa(dti, -1)
            # if a1 > self.max_socket_action or a2 > self.max_socket_action:
            #     self.max_socket_action = np.max([a1, a2, self.max_socket_action])
            #     self.max_socket_action_dt = datetime.now()
            # print(self.max_socket_action_dt, self.max_socket_action, a1, a2)

            if os.path.exists('./stop.txt'):
                os.remove('./stop.txt')
                self.trade_in_progres = False
            else:
                time.sleep(2)

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

    def reg_order(self):
        t = datetime.now().time()
        sec = (t.hour * 60 + t.minute) * 60 + t.second
        self.bal.reg_order_action(sec)
        self.binance_action_limit[0] = self.bal.get_blocked_actions()
        self.binance_action_limit[1] = self.bal.get_sum_orders24(sec)
        self.binance_action_limit[2] = self.bal.get_max_order()
        self.binance_action_limit[3] = self.bal.get_max_request()

    def is_action_limit_ok(self):
        t = datetime.now().time()
        sec = (t.hour * 60 + t.minute) * 60 + t.second
        return self.bal.is_action_limit_ok(sec)

    def order_buy(self, qty_base):
        # {'symbol': 'BTCBUSD',
        # 'orderId': 8210613971,
        # 'orderListId': -1,
        # 'clientOrderId': 'mX56nOCadPtpCuaxUz8Z0r',
        # 'transactTime': 1673972204870,
        # 'price': '0.00000000',
        #  'origQty': '0.00060000',
        #  'executedQty': '0.00060000',
        #  'cummulativeQuoteQty': '12.67965000',
        #  'status': 'FILLED',
        #  'timeInForce': 'GTC',
        #  'type': 'MARKET',
        #  'side': 'BUY',
        #  'workingTime': 1673972204870,
        #  'fills': [{
        #       'price': '21132.75000000',
        #       'qty': '0.00060000',
        #       'commission': '0.00000000',
        #       'commissionAsset': 'BNB',
        #       'tradeId': 784648413}],
        #  'selfTradePreventionMode': 'NONE'}

        if self.simulation:
            time.sleep(0.04934)
            ask = self.actual_ask_price
            return qty_base, qty_base * ask, ask

        dt1 = datetime.now()
        # order_request = self.bx_client.order_market(symbol=self.symbol,
        #                                             side=SIDE_BUY,
        #                                             quantity=str(qty_base))

        # print('qty_base', qty_base)

        order_request = self.bx_client.create_margin_order(symbol=self.symbol,
                                                           side=SIDE_BUY,
                                                           type=ORDER_TYPE_MARKET,
                                                           quantity=str(qty_base),
                                                           isIsolated='TRUE')
        # print(order_request)

        tr_ti = int((datetime.now() - dt1).total_seconds() * 1000)
        # self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)
        self.trade_time[:-1] = self.trade_time[1:]
        self.trade_time[-1] = tr_ti

        executed_qty = round(float(order_request['executedQty']), 4)
        cummulative_quote_qty = float(order_request['cummulativeQuoteQty'])
        self.commission_alert = np.max([float(order_request['fills'][0]['commission']), self.commission_alert])
        traded_price = round(cummulative_quote_qty / executed_qty, 8)

        return executed_qty, cummulative_quote_qty, traded_price

    def order_sell(self, qty_base):
        if self.simulation:
            time.sleep(0.04934)
            bid = self.actual_bid_price
            return qty_base, qty_base * bid, bid

        dt1 = datetime.now()
        # order_request = self.bx_client.order_market(symbol=self.symbol,
        #                                             side=SIDE_SELL,
        #                                             quantity=str(qty_base))

        # print('qty_base', qty_base)

        order_request = self.bx_client.create_margin_order(symbol=self.symbol,
                                                           side=SIDE_SELL,
                                                           type=ORDER_TYPE_MARKET,
                                                           quantity=str(round(qty_base, 4)),
                                                           isIsolated='TRUE')
        # print(order_request)

        tr_ti = int((datetime.now() - dt1).total_seconds() * 1000)
        # self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)
        self.trade_time[:-1] = self.trade_time[1:]
        self.trade_time[-1] = tr_ti

        executed_qty = round(float(order_request['executedQty']), 4)
        cummulative_quote_qty = float(order_request['cummulativeQuoteQty'])
        traded_price = round(cummulative_quote_qty / executed_qty, 8)
        self.commission_alert = np.max([float(order_request['fills'][0]['commission']), self.commission_alert])

        return executed_qty, cummulative_quote_qty, traded_price

    def check_trade_right(self):
        if self.slot_position['qty'] == 0 and not self.trade_in_progres:
            print('AcReA shot down.')
            self.stop_flag[0] = 0
            self.threads_in_progress = False
            return False
        else:
            return True

    def buy(self):

        # shut down hez kell
        if not self.check_trade_right():
            # or not self.sal.is_trade_allow(int(datetime.now().timestamp())):
            return

        # első 20000 adat megérkezéséig nem kötök
        if not self.sal.is_ready_to_start():
            return

        # if self.slot_position['qty'] > 0 and self.slot_position['last_buy_price'] * (1 - self.price_step_delta) < self.actual_ask_price:
        #     return

        ask_price_fixed = self.actual_ask_price
        max_buy_base = round(self.slot_position['free_invest_quote'] / ask_price_fixed, 4)
        # if self.slot_position['qty'] == 0:
        #     calc_qty = round(self.actual_buy_qty_base * 3, 4)
        # else:
        #     calc_qty = round(self.actual_buy_qty_base, 4)
        calc_qty = round(self.actual_buy_qty_base, 4)
        calc_qty = np.min([max_buy_base, calc_qty])

        action_limit_ok = self.is_action_limit_ok()

        # if not self.m_flag and calc_qty >= self.minimum_buy_qty_base and action_limit_ok:
        if calc_qty >= self.minimum_buy_qty_base and action_limit_ok:

            executed_qty, cummulative_quote_qty, traded_price = self.order_buy(calc_qty)

            self.tr.add_trade(executed_qty, traded_price)

            self.slot_position['qty'] = self.tr.get_total_qty()  # new qty
            self.slot_position['free_invest_quote'] -= cummulative_quote_qty
            self.monitor_max_qty = np.max([self.monitor_max_qty, self.slot_position['qty']])
            self.status('max_qty', self.monitor_max_qty, static=True)
            self.status('turnover', cummulative_quote_qty)
            self.slot_position['income_price'] = self.tr.get_open_position_income_price()
            self.slot_position['last_buy_price'] = traded_price
            self.decision_history[-1] = self.decision_long

            self.set_slot_position_array()

            self.reg_order()

    def stop(self, message=""):
        fix_bid_price = self.actual_bid_price
        sell_qty, income_value = self.tr.get_qty(fix_bid_price, spread=0)
        if sell_qty > 0:

            executed_qty, cummulative_quote_qty, traded_price = self.order_sell(sell_qty)
            self.tr.remove_qty(fix_bid_price, spread=0)

            exit_value = cummulative_quote_qty
            self.status('profit', exit_value - income_value)
            self.slot_position['free_invest_quote'] += exit_value
            self.slot_position['qty'] = self.tr.get_total_qty()
            self.slot_position['income_price'] = self.tr.get_open_position_income_price()
            self.status('turnover', exit_value)
            self.decision_history[-1] = self.decision_stop
            self.actual_buy_qty_base = self.start_buy_qty_base
            self.set_slot_position_array()
            self.reg_order()
            self.check_trade_right()

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

    async def add_best_bid_price_history(self, bid):
        self.best_bid_price_history[:-1] = self.best_bid_price_history[1:]
        self.best_bid_price_history[-1] = bid

    async def add_best_ask_price_history(self, ask):
        self.best_ask_price_history[:-1] = self.best_ask_price_history[1:]
        self.best_ask_price_history[-1] = ask

    async def add_decision_history(self):
        self.decision_history[:-1] = self.decision_history[1:]
        self.decision_history[-1] = self.decision_neutral

    @staticmethod
    def back_signal(x):
        # print(x[-100:])
        x = x[::-1]
        b = x[0]
        i_val = b
        i_len = 0
        for ix, d in enumerate(x[1:]):
            if d != b:
                i_val = d
                i_len = ix
                break
        return i_val, i_len

    async def add_bid_uniform_filter(self):
        uf = round((uniform_filter1d(self.best_bid_price_history[-101:], size=100)[-1]) / self.round_block, 0) * self.round_block
        self.uniform_bid_price_history[:-1] = self.uniform_bid_price_history[1:]
        self.uniform_bid_price_history[-1] = uf

    async def add_ask_uniform_filter(self):
        uf = round((uniform_filter1d(self.best_ask_price_history[-101:], size=100)[-1]) / self.round_block, 0) * self.round_block
        self.uniform_ask_price_history[:-1] = self.uniform_ask_price_history[1:]
        self.uniform_ask_price_history[-1] = uf

    async def async_websocket_bookticker_detect(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]

        self.async_client_detect = await AsyncClient.create()
        self.bm_detect = BinanceSocketManager(self.async_client_detect)
        self.ts_detect = self.bm_detect.multiplex_socket(i_socket_list)

        async with self.ts_detect as tscm:
            while self.threads_in_progress:
                res = await tscm.recv()
                self.sal.set_action(int(datetime.now().timestamp()))

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

                asyncio.gather(self.add_best_ask_price_history(ask),
                               self.add_best_bid_price_history(bid),
                               self.add_decision_history())

                asyncio.gather(self.add_bid_uniform_filter(),
                               self.add_ask_uniform_filter())

                if self.uniform_ask_price_history[-2] < self.uniform_ask_price_history[-1] and self.sal.is_ready_to_start():
                    b_value, b_length = self.back_signal(self.uniform_ask_price_history[-3000:-1])
                    if b_value > self.uniform_ask_price_history[-2] and b_length > self.back_length:
                        self.buy()

        await self.async_client_detect.close_connection()

    async def async_websocket_bookticker_stopper(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]

        self.async_client_stopper = await AsyncClient.create()
        self.bm_stopper = BinanceSocketManager(self.async_client_stopper)
        self.ts_stopper = self.bm_stopper.multiplex_socket(i_socket_list)

        async with self.ts_stopper as tscm:
            while self.threads_in_progress:
                res = await tscm.recv()

                # {
                #   "u":400900217,     // order book updateId
                #   "s":"BNBUSDT",     // symbol
                #   "b":"25.35190000", // best bid price
                #   "B":"31.21000000", // best bid qty
                #   "a":"25.36520000", // best ask price
                #   "A":"40.66000000"  // best ask qty
                # }

                ask = float(res['data']['a'])

                self.actual_ask_price = ask

                self.slot_position['actual_value'] = self.slot_position['qty'] * (self.actual_bid_price - self.slot_position['income_price'])
                self.sh_slot_position_array[7] = self.slot_position['actual_value']
                self.slot_position['exit_value'] = self.slot_position['free_invest_quote'] + (self.slot_position['qty'] * self.actual_bid_price)

                # print(self.slot_position_array)
                self.monitor_min_value = np.min([self.monitor_min_value, self.slot_position['actual_value']])
                self.status('min_value', self.monitor_min_value, static=True)
                # Stopper

                if self.uniform_bid_price_history[-2] > self.uniform_bid_price_history[-1] and self.sal.is_ready_to_start():
                    b_value, b_length = self.back_signal(self.uniform_bid_price_history[-3000:-1])
                    if b_value < self.uniform_ask_price_history[-2] and b_length > self.back_length:
                        self.stop()

        await self.async_client_stopper.close_connection()


class Monitor:

    def __init__(self, param):
        self.start_dt = datetime.now()
        self.threads_in_progress = True
        self.stop_flag = param['stop_flag']
        self.name = param['name']
        self.sh_trade_time = param['trade_time']
        self.sh_status_array = param['status_array']
        self.sh_slot_position_array = param['slot_position_array']
        self.sh_binance_action_limit = param['binance_action_limit']

        self.sh_best_bid_price_history = param['best_bid_price_history']
        self.sh_best_ask_price_history = param['best_ask_price_history']
        self.sh_uniform_bid_price_history = param['uniform_bid_price_history']
        self.sh_uniform_ask_price_history = param['uniform_ask_price_history']

        self.sh_decision_history = param['decision_history']

        self.time_period = param['time_period']

        self.transfer = np.array([[0.0] * self.time_period] * 8)

        self.process = param['process']
        self.cores = param['cores']
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        self.data_manager()

    def data_transfer(self):

        self.transfer[0][:] = np.array(self.sh_best_bid_price_history[:])
        self.transfer[1][:] = np.array(self.sh_best_ask_price_history[:])
        self.transfer[5][:] = np.array(self.sh_uniform_bid_price_history[:])
        self.transfer[6][:] = np.array(self.sh_uniform_ask_price_history[:])
        self.transfer[7][:] = np.array(self.sh_decision_history[:])

        np.savez_compressed("./sync_local/transfer_all.npz",
                            array1=self.sh_slot_position_array,
                            array2=self.sh_trade_time,
                            array3=self.sh_status_array,
                            array4=self.sh_binance_action_limit,
                            array5=self.transfer,
                            array6=np.array([self.name, str(self.start_dt)]),
                            )

        # print('prc1', psutil.cpu_percent(interval=0.1, percpu=True))
        # print('prc4', psutil.cpu_percent(interval=4, percpu=True))

    # def print_data(self):
    #     print(self.sh_best_bid_price_history[:][-100:])
    #
    #     slot_position_dict = {'qty': round(self.sh_slot_position_array[0], 8),
    #                           'income_price': round(self.sh_slot_position_array[1], 8),
    #                           'free_invest_quote': round(self.sh_slot_position_array[2], 8),
    #                           'stop_price': round(self.sh_slot_position_array[3], 8),
    #                           'trailer_stop_price': round(self.sh_slot_position_array[4], 8),
    #                           'trailer_minimum_price': round(self.sh_slot_position_array[5], 8),
    #                           'take_price': round(self.sh_slot_position_array[6], 8),
    #                           'actual_value_quote': round(self.sh_slot_position_array[7], 8),
    #                           'actual_profile': round(self.sh_slot_position_array[8], 8),
    #                           'last_buy_price': round(self.sh_slot_position_array[9], 8),
    #                           }
    #
    #     status_array_dict = {'stop': int(self.sh_status_array[0]),
    #                          'buy_again': int(self.sh_status_array[1]),
    #                          'take': int(self.sh_status_array[2]),
    #                          'trailer': int(self.sh_status_array[3]),
    #                          'time': int(self.sh_status_array[4]),
    #                          'profit': round(self.sh_status_array[5], 8),
    #                          'turnover': round(self.sh_status_array[6], 8),
    #                          'max_qty': round(self.sh_status_array[7], 8),
    #                          'min_value_qoute': round(self.sh_status_array[8], 8)
    #                          }
    #
    #     print(slot_position_dict)
    #     print(status_array_dict)
    #
    #     print("    get_blocked_actions total", self.sh_binance_action_limit[0])
    #     print("get_sum_orders24 160000 / 24h", self.sh_binance_action_limit[1])
    #     print("     get_max_order 50 / 10sec", self.sh_binance_action_limit[2])
    #     print(" get_max_request 1200 / 60sec", self.sh_binance_action_limit[3])
    #     print(self.sh_trade_time[:])

    def data_manager(self):
        while self.threads_in_progress:
            if self.stop_flag[0] == 0:
                self.threads_in_progress = False
            time.sleep(3)
            # self.print_data()
            self.data_transfer()

        print('Monitor shutdown.')
        # self.print_data()


# __main__
if __name__ == '__main__':
    path = "./venv/"
    if os.path.exists(path):
        print('Local running?')
        sys.exit()

    cores = cpu_count()
    running_processes = []

    ###################
    #  shared memory  #
    ###################

    time_period = 20000

    status_array = Array('f', [0.0] * 11)
    slot_position_array = Array('f', [0.0] * 11)
    binance_action_limit = Array('i', [0] * 4)
    trade_time = Array('i', [0] * 100)
    decision_history = Array('i', [0] * time_period)

    best_bid_price_history = Array('f', [0.0] * time_period)
    best_ask_price_history = Array('f', [0.0] * time_period)
    uniform_bid_price_history = Array('f', [0.0] * time_period)
    uniform_ask_price_history = Array('f', [0.0] * time_period)
    stop_flag = Array('i', [1])

    # 4
    # 3
    # 2
    # 1

    setting = "slow1000"
    print(setting)
    params = {}
    if setting == "slow1000":
        params = {'simulation': True,
                  'name': "slow1000",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.001,
                  'back_length': 25,
                  'round_block': 1.25,
                  'buy_multiplier': 1,
                  'time_period': time_period,
                  'status_array': status_array,
                  'slot_position_array': slot_position_array,
                  'binance_action_limit': binance_action_limit,
                  'trade_time': trade_time,
                  'best_bid_price_history': best_bid_price_history,
                  'best_ask_price_history': best_ask_price_history,
                  'uniform_bid_price_history': uniform_bid_price_history,
                  'uniform_ask_price_history': uniform_ask_price_history,
                  'decision_history': decision_history,
                  'stop_flag': stop_flag,

                  }

    n_acrea = GridAcReA
    process1 = Process(target=n_acrea, args=(params,))
    process1.start()

    params2 = {'cores': cores,
               'name': params['name'],
               'process': 2,
               'base': "BTC",
               'quote': "USDT",
               'time_period': time_period,
               'status_array': status_array,
               'slot_position_array': slot_position_array,
               'binance_action_limit': binance_action_limit,
               'trade_time': trade_time,
               'best_bid_price_history': best_bid_price_history,
               'best_ask_price_history': best_ask_price_history,
               'uniform_bid_price_history': uniform_bid_price_history,
               'uniform_ask_price_history': uniform_ask_price_history,
               'decision_history': decision_history,
               'stop_flag': stop_flag,

               }

    n_monitor = Monitor
    process2 = Process(target=n_monitor, args=(params2,))
    process2.start()

    process1.join()
    process2.join()
