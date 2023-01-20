import sys
import time
import pickle
from datetime import datetime, timedelta
import psutil

from numba import int32, float32
from numba.experimental import jitclass
from numba import njit, objmode, void
import numba
# import psutil

import asyncio
from threading import Thread
from multiprocessing import cpu_count, Process, Array, Queue

import numpy as np
from scipy.signal import savgol_filter

from binance import AsyncClient, BinanceSocketManager, Client
# from binance.exceptions import BinanceAPIException
from binance.enums import *

# from sklearn import linear_model
# from sklearn.preprocessing import PolynomialFeatures
# from sklearn.pipeline import make_pipeline
# from sklearn.linear_model import LinearRegression

np.set_printoptions(threshold=5000)

spec = [
    ('request_limit_sec', int32),
    ('request_limit_action', int32),
    ('order_limit_1_sec', int32),
    ('order_limit_1_action', int32),
    ('order_limit_2_action', int32),
    ('count_block_action', int32),
    ('request_reg', int32[:]),
    ('request_reg_yesterday', int32[:]),
    ('order_reg', int32[:]),
    ('order_reg_yesterday', int32[:]),
]

@jitclass(spec)
class BinanceActionLimit:

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


class AcReA:
    def __init__(self, param):
        self.deposit_quote = param['deposit_quote']
        self.server_start_dt = datetime.now()
        self.binance_action_limit = param['binance_action_limit']
        self.trade_time = param['trade_time']
        self.bal = BinanceActionLimit()
        self.commission_alert = 0

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

        self.api_key = "sNtEg0vnKFm09xKf8v9VJWIYspFFouJN5vJO9bSgUSAU8eoAUa5OaMuJYLLsuswr"
        self.api_secret = "Hhu6MPTOdEKBjZyooPr1JxbgiLME3VFJdymqJrtytrMywKatP08Y5G1Sb9ZuJv4S"
        self.bx_client = Client(self.api_key, self.api_secret)

        self.async_client = None
        self.bm = None
        self.ts = None

        self.async_client2 = None
        self.bm2 = None
        self.ts2 = None

        self.ntick = .5 / 15000
        self.time_period = param['time_period']
        self.slot = {'stop_delta': 0.0,
                     'trailer_delta': 0.0,
                     'take_delta': 0.0,
                     'max_time_sec': 0,
                     }

        ddown_state = 4.25
        ddown_multiplier = 1.5
        self.trading_profile = {
            1: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 12,
                'renko_slow_steps': 2,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -3400,
                },
            2: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 12,
                'renko_slow_steps': 2,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3400,
            },
            3: {
                'stop_delta': self.ntick * 3,
                'trailer_delta': self.ntick * 1,
                'take_delta': self.ntick * 8,
                'max_time_sec': 60 * 12,
                'renko_slow_steps': 2,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3400,
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
                              'exit_value': 0.0,
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
                                    'exit_value': 10,
                                    }

        # két esetben direkt címzem (4,7) a többi esetben set_slot_position_array használom
        # két esetben számít a sebesség

        self.sh_slot_position_array = param['slot_position_array']

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

        self.m_flag = False

        self.best_bid_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.best_ask_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.renko_slow_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.renko_fast_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.renko_stop_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.smoot_slow_price_history = np.array([0.0] * self.time_period, dtype=np.float32)
        self.smoot_fast_price_history = np.array([0.0] * self.time_period, dtype=np.float32)

        self.sh_best_bid_price_history = param['best_bid_price_history']
        self.sh_best_ask_price_history = param['best_ask_price_history']
        self.sh_renko_slow_price_history = param['renko_slow_price_history']
        self.sh_renko_fast_price_history = param['renko_fast_price_history']
        self.sh_renko_stop_price_history = param['renko_stop_price_history']
        self.sh_smoot_slow_price_history = param['smoot_slow_price_history']
        self.sh_smoot_fast_price_history = param['smoot_fast_price_history']

        self.decision_neutral = 0
        self.decision_long = 500
        self.decision_stop = -500
        self.decision_buy_again = 250

        self.decision_history = np.array([0] * self.time_period, dtype=np.int32)
        self.sh_decision_history = param['decision_history']

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
                                   'min_value': 8,
                                   }

        self.sh_status_array = param['status_array']

        self.actual_profile = 1
        self.set_profile(1, direct=True)

        self.start_threads()

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

        self.sh_status_array[9] = round(self.sh_status_array[5] / self.time_delta * (24*60*60*365) / self.deposit_quote, 4)

    def set_profile(self, p, direct=False):
        if p != self.actual_profile < p or direct:
            self.slot['stop_delta'] = self.trading_profile[p]['stop_delta']
            self.slot['trailer_delta'] = self.trading_profile[p]['trailer_delta']
            self.slot['take_delta'] = self.trading_profile[p]['take_delta']
            self.slot['max_time_sec'] = self.trading_profile[p]['max_time_sec']
            # self.slot['extra_time_sec'] = self.trading_profile[p]['extra_time_sec']

            self.renko_slow_steps = self.trading_profile[p]['renko_slow_steps']
            self.renko_fast_steps = self.trading_profile[p]['renko_fast_steps']
            self.renko_stop_steps = self.trading_profile[p]['renko_stop_steps']
            self.ddown_limit = self.trading_profile[p]['ddown_limit']
            self.ddown_points = np.arange(self.trading_profile[p]['ddown_depth'], -1, 20).astype(np.int32)
            self.actual_profile = p
            self.slot_position['actual_profile'] = p * 1.0
            self.set_slot_position_array()

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

    # def moving_average(self, x, w):
    #     iret = np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])
    #     return iret

    def data_transfer(self):
        while True:
            self.sh_best_bid_price_history[:] = self.best_bid_price_history[:]
            self.sh_best_ask_price_history[:] = self.best_ask_price_history[:]
            self.sh_renko_slow_price_history[:] = self.renko_slow_price_history[:]
            self.sh_renko_fast_price_history[:] = self.renko_fast_price_history[:]
            self.sh_renko_stop_price_history[:] = self.renko_stop_price_history[:]
            self.sh_smoot_slow_price_history[:] = self.smoot_slow_price_history[:]
            self.sh_smoot_fast_price_history[:] = self.smoot_fast_price_history[:]
            self.sh_decision_history[:] = self.decision_history[:]
            time.sleep(3)

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

        dt1 = datetime.now()
        order_request = self.bx_client.order_market(symbol=self.symbol,
                                                    side=SIDE_BUY,
                                                    quantity=str(qty_base))
        tr_ti = int((datetime.now() - dt1).total_seconds() * 1000)
        self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)

        executed_qty = float(order_request['executedQty'])
        cummulative_quote_qty = float(order_request['cummulativeQuoteQty'])
        self.commission_alert = np.max([float(order_request['fills'][0]['commission']), self.commission_alert])
        traded_price = round(cummulative_quote_qty / executed_qty, 8)

        return executed_qty, cummulative_quote_qty, traded_price

    def order_sell(self, qty_base):
        dt1 = datetime.now()
        order_request = self.bx_client.order_market(symbol=self.symbol,
                                                    side=SIDE_SELL,
                                                    quantity=str(qty_base))
        tr_ti = int((datetime.now() - dt1).total_seconds() * 1000)
        self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)

        executed_qty = float(order_request['executedQty'])
        cummulative_quote_qty = float(order_request['cummulativeQuoteQty'])
        traded_price = round(cummulative_quote_qty / executed_qty, 8)
        self.commission_alert = np.max([float(order_request['fills'][0]['commission']), self.commission_alert])

        return executed_qty, cummulative_quote_qty, traded_price

    def buy(self):
        if self.slot_position['qty'] > 0 and self.slot_position['last_buy_price'] < self.actual_ask_price:
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)
            return
        time.sleep(0.015)

        ask_price_fixed = self.actual_ask_price
        max_buy_base = round(self.slot_position['free_invest_quote'] / ask_price_fixed, 4)
        calc_qty = round(self.actual_buy_qty_base, 4)
        calc_qty = np.min([max_buy_base, calc_qty])

        action_limit_ok = self.is_action_limit_ok()

        if not self.m_flag and calc_qty >= self.minimum_buy_qty_base and action_limit_ok:

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
            # self.slot_position['extra_dt'] = self.slot_position['exit_dt'] + timedelta(seconds=self.slot['extra_time_sec'])
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

            self.reg_order()

            self.m_flag = True
        elif not self.m_flag and calc_qty >= self.minimum_buy_qty_base and not action_limit_ok:
            # ha blokkolta, action limit miatt, akkor növeli a következő vételi mennyiséget
            self.actual_buy_qty_base *= self.buy_multiplier
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)
        else:
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)
            # print("")
            # print("Buy", symbol, slot, qty, self.slot_position['income_price'], self.actual_ask_qty[symbol])

    def stop(self, message=""):
        if self.slot_position['income_price'] > self.actual_bid_price or message != "Time":
            time.sleep(0.015)
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
            self.reg_order()
            self.m_flag = False
        else:
            print(message)
            self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)

    def allowed_buy_again(self):
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_buy_again], axis=0), 0)
        self.actual_buy_qty_base *= self.buy_multiplier
        self.m_flag = False

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

    async def add_renko_slow_price_history(self, ask):
        if self.renko_slow_steps <= abs(self.renko_slow_price_history[-1] - ask):
            self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [ask], axis=0), 0)
        else:
            self.renko_slow_price_history = np.delete(np.append(self.renko_slow_price_history, [self.renko_slow_price_history[-1]], axis=0), 0)

    async def add_renko_fast_price_history(self, ask):
        if self.renko_fast_steps <= abs(self.renko_fast_price_history[-1] - ask):
            self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [ask], axis=0), 0)
        else:
            self.renko_fast_price_history = np.delete(np.append(self.renko_fast_price_history, [self.renko_fast_price_history[-1]], axis=0), 0)

    async def set_down(self):
        ddown_array = np.array(self.smoot_slow_price_history)
        ddown = ddown_array[self.ddown_points] - self.smoot_slow_price_history[-1]
        self.ddown = ddown > self.ddown_limit

    async def add_fast_savgol(self):
        savgol_last = savgol_filter(self.renko_fast_price_history[-600:], 300, 1)[-1]
        self.smoot_fast_price_history = np.delete(np.append(self.smoot_fast_price_history, [savgol_last], axis=0), 0)

    async def add_slow_savgol(self):
        savgol_last = savgol_filter(self.renko_slow_price_history[-600:], 300, 1)[-1]
        self.smoot_slow_price_history = np.delete(np.append(self.smoot_slow_price_history, [savgol_last], axis=0), 0)

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

                asyncio.gather(self.add_best_ask_price_history(ask),
                               self.add_best_bid_price_history(bid),
                               self.add_renko_fast_price_history(ask),
                               self.add_renko_slow_price_history(ask))

                asyncio.gather(self.add_fast_savgol(),
                               self.add_slow_savgol(),
                               self.set_down())

                # if self.trough_detect(self.sh_smoot_fast_price_history) and self.ddown.any():
                if self.smoot_fast_price_history[-1] > self.smoot_fast_price_history[-2] and self.ddown.any():
                # if self.trough_detect(self.sh_smoot_fast_price_history):
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
                self.sh_slot_position_array[7] = self.slot_position['actual_value']
                self.slot_position['exit_value'] = self.slot_position['free_invest_quote'] + (self.slot_position['qty'] * self.actual_bid_price)

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
                    self.sh_slot_position_array[4] = self.slot_position['trailer_stop_price']

                    if self.slot_position['take_price'] <= selected_price and self.smoot_slow_price_history[-1] < self.smoot_slow_price_history[-2]:
                        self.stop("Take")
                        self.status('take', 1)

                        # if self.actual_bid_price[symbol] - self._slot_position['income_price'] > 2:
                        #     self._slot_position['stop_price'] = (self._slot_position['income_price'] + self.actual_bid_price[symbol]) / 5 * 4
                        # else:
                        #     self._slot_position['stop_price'] = (self._slot_position['income_price'] + self.actual_bid_price[symbol]) / 2
                        # self._slot_position['take_price'] = self.actual_bid_price[symbol] * (1 + self.slot['take_delta'])

                    elif selected_price <= self.slot_position['stop_price'] and self.m_flag:
                        # print("    bid", self.actual_bid_price[symbol], "stop   ", self.slot_position['stop_price'])
                        # if self.m_flag:
                        self.allowed_buy_again()
                        self.status('buy_again', 1)

                    elif self.slot_position['exit_dt'] < datetime.now():
                        self.stop("Time")
                        self.status('time', 1)
                        #
                        # if selected_price > self.slot_position['income_price']:
                        #     self.slot_position['exit_dt'] = self.slot_position['exit_dt'] + timedelta(seconds=self.slot['extra_time_sec'])
                        #
                        # # and not self._slot_position['extra_flag']:
                        # # self._slot_position['extra_flag'] = True
                        # # if self.actual_bid_price[symbol] > self._slot_position['income_price']:
                        # #     income_value = self._slot_position['qty'] * self._slot_position['income_price']
                        # #     exit_value = self._slot_position['qty'] * self.actual_bid_price[symbol]
                        # #     print("Actual profit at reset:", exit_value - income_value)
                        # #     self.reset_extra_profit(symbol, slot)
                        # else:
                        #     self.stop("Time")
                        #     self.status('time', 1)

                    elif self.slot_position['trailer_minimum_price'] <= selected_price <= self.slot_position['trailer_stop_price'] \
                            and self.smoot_fast_price_history[-1] < self.smoot_fast_price_history[-2]:
                        self.stop("Trailer")
                        self.status('trailer', 1)


class Monitor:

    def __init__(self, param):
        self.sh_trade_time = param['trade_time']
        self.sh_status_array = param['status_array']
        self.sh_slot_position_array = param['slot_position_array']
        self.sh_binance_action_limit = param['binance_action_limit']

        self.sh_best_bid_price_history = param['best_bid_price_history']
        self.sh_best_ask_price_history = param['best_ask_price_history']
        self.sh_renko_slow_price_history = param['renko_slow_price_history']
        self.sh_renko_fast_price_history = param['renko_fast_price_history']
        self.sh_renko_stop_price_history = param['renko_stop_price_history']
        self.sh_smoot_slow_price_history = param['smoot_slow_price_history']
        self.sh_smoot_fast_price_history = param['smoot_fast_price_history']

        self.sh_decision_history = param['decision_history']

        self.time_period = param['time_period']

        self.transfer = np.array([[0.0] * self.time_period] * 8)

        self.process = param['process']
        self.cores = param['cores']
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        self.data_manager()

    @staticmethod
    def moving_average(x, w):
        return np.concatenate([np.array([x[0]] * (w - 1)), np.convolve(x, np.ones(w), 'valid') / w])

    def data_transfer(self):

        self.transfer[0][:] = np.array(self.sh_best_bid_price_history[:])
        self.transfer[1][:] = np.array(self.sh_best_ask_price_history[:])
        self.transfer[2][:] = np.array(self.sh_renko_slow_price_history[:])
        self.transfer[3][:] = np.array(self.sh_renko_fast_price_history[:])
        self.transfer[4][:] = np.array(self.sh_renko_stop_price_history[:])
        self.transfer[5][:] = np.array(self.sh_smoot_slow_price_history[:])
        self.transfer[6][:] = np.array(self.sh_smoot_fast_price_history[:])
        self.transfer[7][:] = np.array(self.sh_decision_history[:])

        np.savez_compressed("transfer_timeseries.npz", self.transfer[:])
        np.savez_compressed("transfer_sh_slot_position_array.npz", self.sh_slot_position_array[:])
        np.savez_compressed("transfer_sh_trade_time.npz", self.sh_trade_time[:])
        np.savez_compressed("transfer_sh_status_array.npz", self.sh_status_array[:])
        np.savez_compressed("transfer_sh_binance_action_limit.npz", self.sh_binance_action_limit[:])

        # print('prc1', psutil.cpu_percent(interval=0.1, percpu=True))
        # print('prc4', psutil.cpu_percent(interval=4, percpu=True))

    def print_data(self):
        print(self.sh_best_bid_price_history[:][-100:])

        slot_position_dict = {'qty': round(self.sh_slot_position_array[0], 8),
                              'income_price': round(self.sh_slot_position_array[1], 8),
                              'free_invest_quote': round(self.sh_slot_position_array[2], 8),
                              'stop_price': round(self.sh_slot_position_array[3], 8),
                              'trailer_stop_price': round(self.sh_slot_position_array[4], 8),
                              'trailer_minimum_price': round(self.sh_slot_position_array[5], 8),
                              'take_price': round(self.sh_slot_position_array[6], 8),
                              'actual_value_quote': round(self.sh_slot_position_array[7], 8),
                              'actual_profile': round(self.sh_slot_position_array[8], 8),
                              'last_buy_price': round(self.sh_slot_position_array[9], 8),
                              }

        status_array_dict = {'stop': int(self.sh_status_array[0]),
                             'buy_again': int(self.sh_status_array[1]),
                             'take': int(self.sh_status_array[2]),
                             'trailer': int(self.sh_status_array[3]),
                             'time': int(self.sh_status_array[4]),
                             'profit': round(self.sh_status_array[5], 8),
                             'turnover': round(self.sh_status_array[6], 8),
                             'max_qty': round(self.sh_status_array[7], 8),
                             'min_value_qoute': round(self.sh_status_array[8], 8)
                             }

        print(slot_position_dict)
        print(status_array_dict)

        print("    get_blocked_actions total", self.sh_binance_action_limit[0])
        print("get_sum_orders24 160000 / 24h", self.sh_binance_action_limit[1])
        print("     get_max_order 50 / 10sec", self.sh_binance_action_limit[2])
        print(" get_max_request 1200 / 60sec", self.sh_binance_action_limit[3])
        print(self.sh_trade_time[:])

    def data_manager(self):
        while True:
            time.sleep(1)
            # self.print_data()
            self.data_transfer()


if __name__ == '__main__':
    cores = cpu_count()
    running_processes = []

    ###################
    #  shared memory  #
    ###################

    time_period = 20000

    status_array = Array('f', [0.0] * 10)
    slot_position_array = Array('f', [0.0] * 11)
    binance_action_limit = Array('i', [0] * 4)
    trade_time = Array('i', [0] * 100)
    decision_history = Array('i', [0] * time_period)
    # decision_history = Queue(maxsize=time_period)

    best_bid_price_history = Array('f', [0.0] * time_period)
    best_ask_price_history = Array('f', [0.0] * time_period)
    renko_slow_price_history = Array('f', [0.0] * time_period)
    renko_fast_price_history = Array('f', [0.0] * time_period)
    renko_stop_price_history = Array('f', [0.0] * time_period)
    smoot_slow_price_history = Array('f', [0.0] * time_period)
    smoot_fast_price_history = Array('f', [0.0] * time_period)

    n_acrea = AcReA
    n_monitor = Monitor

    params = {'cores': cores,
              'process': 1,
              'base': "BTC",
              'quote': "BUSD",
              'deposit_quote': 1000,
              'max_invest_quote': 1000 * 4,
              'minimum_buy_qty_base': 0.0005,
              'buy_multiplier': 1.05,
              'trade_profile_limits': [.1, .3],
              'time_period': time_period,
              'status_array': status_array,
              'slot_position_array': slot_position_array,
              'binance_action_limit': binance_action_limit,
              'trade_time': trade_time,
              'best_bid_price_history': best_bid_price_history,
              'best_ask_price_history': best_ask_price_history,
              'renko_slow_price_history': renko_slow_price_history,
              'renko_fast_price_history': renko_fast_price_history,
              'renko_stop_price_history': renko_stop_price_history,
              'smoot_slow_price_history': smoot_slow_price_history,
              'smoot_fast_price_history': smoot_fast_price_history,
              'decision_history': decision_history,

              }

    process1 = Process(target=n_acrea, args=(params,))
    process1.start()

    params2 = {'cores': cores,
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
               'renko_slow_price_history': renko_slow_price_history,
               'renko_fast_price_history': renko_fast_price_history,
               'renko_stop_price_history': renko_stop_price_history,
               'smoot_slow_price_history': smoot_slow_price_history,
               'smoot_fast_price_history': smoot_fast_price_history,
               'decision_history': decision_history,

               }

    process2 = Process(target=n_monitor, args=(params2,))
    process2.start()

    # process1.join()
    # process2.join()

    while True:
        time.sleep(1000)
