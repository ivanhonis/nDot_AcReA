import sys
import time
import os
# import pickle
from datetime import datetime, timedelta
# import psutil

from numba import int32, types, typed
from numba.experimental import jitclass
# from numba import njit, objmode, void
# import numba
# import psutil

import asyncio
from threading import Thread
from multiprocessing import cpu_count, Process, Array
# import subprocess

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


time_dict_type = (types.int64, types.int64)
spec = [
    ('socket_action', types.DictType(*time_dict_type)),
    ('trade_allowe_timestamp', int32),
    ('trade_ban', int32),
    ('speed_limit', int32),
]


@jitclass(spec)
class SocketActionLimit:

    def __init__(self):
        self.socket_action = typed.Dict.empty(*time_dict_type)
        self.trade_allowe_timestamp = 0
        self.trade_ban = 5  # sec
        self.speed_limit = 350  # socket action per sec

    def set_action(self, timestamp):
        if timestamp in self.socket_action:
            self.socket_action[timestamp] += 1
        else:
            self.socket_action[timestamp] = 1

        if self.socket_action[timestamp] > 350:
            self.trade_allowe_timestamp = timestamp + self.trade_ban

        if timestamp - 3 in self.socket_action:
            del self.socket_action[timestamp - 3]

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
        self.stop_flag = param['stop_flag']
        self.trade_in_progres = True
        self.threads_in_progress = True

        self.sh_status_array = param['status_array']
        self.simulation = param['simulation']
        self.reg_trading_mode()

        self.socket_counter = 0         # addig nem trédel amíg nem megy minim 2000 + 5000 adat
        self.socket_extra_delay = 5000
        self.sal = SocketActionLimit()
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

        self.trade_profile_limits = np.array(param['trade_profile_limits'])  # a befektetésével (beáramlásával) állítja a profil szintek (%)

        self.api_key = "sNtEg0vnKFm09xKf8v9VJWIYspFFouJN5vJO9bSgUSAU8eoAUa5OaMuJYLLsuswr"
        self.api_secret = "Hhu6MPTOdEKBjZyooPr1JxbgiLME3VFJdymqJrtytrMywKatP08Y5G1Sb9ZuJv4S"
        self.bx_client = Client(self.api_key, self.api_secret)

        self.async_client_detect = None
        self.bm_detect = None
        self.ts_detect = None

        self.async_client_stopper = None
        self.bm_stopper = None
        self.ts_stopper = None

        self.time_period = param['time_period']
        self.slot = {'stop_delta': 0.0,
                     'trailer_delta': 0.0,
                     'take_delta': 0.0,
                     'max_time_sec': 0,
                     }

        self.trading_profile = param['trading_profile']
        self._ipd2_s = self. trading_profile[2]['income_price_distance'][0]
        self._ipd2_n = self. trading_profile[2]['income_price_distance'][1]
        self._ipd3_s = self. trading_profile[3]['income_price_distance'][0]
        self._ipd3_n = self. trading_profile[3]['income_price_distance'][1]

        self.slot_position = {'qty': 0.0,
                              'income_price': 0.0,
                              'free_invest_quote': self.max_invest_quote,
                              'stop_price': 0.0,
                              'trailer_stop_price': 0.0,
                              'trailer_minimum_price': 0.0,
                              'take_price': 0.0,
                              'enter_dt': None,
                              'exit_dt': None,
                              # 'extra_dt': None,
                              # 'extra_flag': False,
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

        # utolsó ismert orderbook legjobb adatai
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
        self.price_step_delta = 0.0

        self.m_flag = False  # ez ad engedélyt arra hogy újra vásároljunk

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

        self.actual_profile = 1
        self.set_profile(1, direct=True)

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

    def min_max_bid(self, x, y):
        return (np.max(self.best_bid_price_history) - np.min(self.best_bid_price_history)) / y * x

    def min_max_ask(self, x, y):
        return (np.max(self.best_ask_price_history) - np.min(self.best_ask_price_history)) / y * x

    def max_bid(self):
        return np.max(self.best_bid_price_history)

    def max_ask(self):
        return np.max(self.best_ask_price_history)

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
            self.price_step_delta = self.trading_profile[p]['price_step_delta']
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
        while self.threads_in_progress:
            self.sh_best_bid_price_history[:] = self.best_bid_price_history[:]
            self.sh_best_ask_price_history[:] = self.best_ask_price_history[:]
            self.sh_renko_slow_price_history[:] = self.renko_slow_price_history[:]
            self.sh_renko_fast_price_history[:] = self.renko_fast_price_history[:]
            self.sh_renko_stop_price_history[:] = self.renko_stop_price_history[:]
            self.sh_smoot_slow_price_history[:] = self.smoot_slow_price_history[:]
            self.sh_smoot_fast_price_history[:] = self.smoot_fast_price_history[:]
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
        self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)

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
        self.trade_time = np.delete(np.append(self.trade_time, [tr_ti], axis=0), 0)

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
        if not self.check_trade_right() or not self.sal.is_trade_allow(int(datetime.now().timestamp())):
            return

        if self.socket_counter < self.time_period + self.socket_extra_delay:
            return

        if self.slot_position['qty'] > 0 and self.slot_position['last_buy_price'] * (1 - self.price_step_delta) < self.actual_ask_price:
            return

        ask_price_fixed = self.actual_ask_price
        max_buy_base = round(self.slot_position['free_invest_quote'] / ask_price_fixed, 4)
        if self.slot_position['qty'] == 0:
            calc_qty = round(self.actual_buy_qty_base * 3, 4)
        else:
            calc_qty = round(self.actual_buy_qty_base, 4)
        calc_qty = np.min([max_buy_base, calc_qty])

        action_limit_ok = self.is_action_limit_ok()

        if not self.m_flag and calc_qty >= self.minimum_buy_qty_base and action_limit_ok:

            executed_qty, cummulative_quote_qty, traded_price = self.order_buy(calc_qty)

            existed_value = self.slot_position['income_price'] * self.slot_position['qty']
            # new_value = calc_qty * ask_price_fixed
            new_value = cummulative_quote_qty

            # self.slot_position['qty'] += calc_qty  # new qty
            self.slot_position['qty'] += executed_qty  # new qty
            # print("self.slot_position['qty']", self.slot_position['qty'])
            self.slot_position['free_invest_quote'] -= new_value
            self.monitor_max_qty = np.max([self.monitor_max_qty, self.slot_position['qty']])
            self.status('max_qty', self.monitor_max_qty, static=True)
            # self.status('turnover', calc_qty * ask_price_fixed)
            self.status('turnover', cummulative_quote_qty)
            self.slot_position['income_price'] = (existed_value + new_value) / self.slot_position['qty']
            # self.slot_position['last_buy_price'] = ask_price_fixed
            self.slot_position['last_buy_price'] = traded_price
            self.slot_position['stop_price'] = self.slot_position['income_price'] * (1 - self.slot['stop_delta'])
            self.slot_position['trailer_stop_price'] = self.slot_position['income_price']  # innen indul és kezdi emelgetni
            self.slot_position['trailer_minimum_price'] = self.slot_position['income_price'] * (1 + self.slot['trailer_delta'])
            self.slot_position['take_price'] = self.slot_position['income_price'] * (1 + self.slot['take_delta'])
            self.slot_position['enter_dt'] = datetime.now()
            self.slot_position['exit_dt'] = self.slot_position['enter_dt'] + timedelta(seconds=self.slot['max_time_sec'])
            # self.slot_position['extra_dt'] = self.slot_position['exit_dt'] + timedelta(seconds=self.slot['extra_time_sec'])
            # self.slot_position['extra_flag'] = False

            # Risk management
            position_value = self.slot_position['qty'] * self.actual_bid_price
            total_exit_value = self.slot_position['free_invest_quote'] + position_value
            if self.trade_profile_limits[0] <= position_value / total_exit_value < self.trade_profile_limits[1]:
                self.set_profile(2)
            elif self.trade_profile_limits[1] <= position_value / total_exit_value:
                self.set_profile(3)

            self.decision_history[-1] = self.decision_long

            self.set_slot_position_array()

            self.reg_order()

            self.m_flag = True
        elif not self.m_flag and calc_qty >= self.minimum_buy_qty_base and not action_limit_ok:
            # ha blokkolta, action limit miatt, akkor növeli a következő vételi mennyiséget
            self.actual_buy_qty_base *= self.buy_multiplier

        self.socket_counter = self.time_period + self.socket_extra_delay + 100

    def stop(self, message=""):
        if self.slot_position['income_price'] > self.actual_bid_price or message != "Time":

            executed_qty, cummulative_quote_qty, traded_price = self.order_sell(self.slot_position['qty'])

            income_value = self.slot_position['qty'] * self.slot_position['income_price']
            # exit_value = self.slot_position['qty'] * self.actual_bid_price
            exit_value = cummulative_quote_qty
            self.status('profit', exit_value - income_value)
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
            # self.slot_position['extra_dt'] = None
            # self.slot_position['extra_flag'] = False
            # self.monitor_fee += round((income_value * 0.025 / 100) + (exit_value * 0.025 / 100), 2)
            self.status('turnover', exit_value)
            self.decision_history[-1] = self.decision_stop
            self.actual_buy_qty_base = self.start_buy_qty_base
            self.set_profile(1, direct=True)
            self.set_slot_position_array()
            self.reg_order()
            self.m_flag = False
            self.check_trade_right()

    def allowed_buy_again(self):
        self.decision_history[-1] = self.decision_buy_again
        self.actual_buy_qty_base *= self.buy_multiplier
        self.m_flag = False

    # def slot_in_position(self):
    #     if self.slot_position['qty'] > 0:
    #         return True
    #     elif self.slot_position['qty'] == 0:
    #         return False
    #     else:
    #         print("Minusz pozíció!!!!!")
    #         # TODO ezt a hibát kezelni kell
    #         sys.exit()

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

    async def add_decision_history(self):
        self.decision_history = np.delete(np.append(self.decision_history, [self.decision_neutral], axis=0), 0)

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
        # self.ddown = ddown > self.ddown_limit
        self.ddown = ddown > self.ddown_limit

    async def add_fast_savgol(self):
        savgol_last = savgol_filter(self.renko_fast_price_history[-600:], 300, 1)[-1]
        self.smoot_fast_price_history = np.delete(np.append(self.smoot_fast_price_history, [savgol_last], axis=0), 0)

    async def add_slow_savgol(self):
        savgol_last = savgol_filter(self.renko_slow_price_history[-600:], 300, 1)[-1]
        self.smoot_slow_price_history = np.delete(np.append(self.smoot_slow_price_history, [savgol_last], axis=0), 0)

    async def async_websocket_bookticker_detect(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]

        self.async_client_detect = await AsyncClient.create()
        self.bm_detect = BinanceSocketManager(self.async_client_detect)
        self.ts_detect = self.bm_detect.multiplex_socket(i_socket_list)

        # r1 = -10
        # r2 = -40

        async with self.ts_detect as tscm:
            while self.threads_in_progress:
                res = await tscm.recv()
                self.socket_counter += 1
                self.sal.set_action(int(datetime.now().timestamp()))
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
                               self.add_renko_slow_price_history(ask),
                               self.add_decision_history())

                asyncio.gather(self.add_fast_savgol(),
                               self.add_slow_savgol(),
                               self.set_down())

                #TODO tedd be a speed limit ellenőrzést  a set_down tedd utána azéert hogy bann esetén ki se értékelje

                # if self.trough_detect(self.sh_smoot_fast_price_history) and self.ddown.any():
                if self.actual_profile == 1 and self.smoot_fast_price_history[-1] > self.smoot_fast_price_history[-2] and self.ddown.any() \
                        and self.max_ask() - self.min_max_ask(1, 4) > self.actual_ask_price:
                    self.buy()
                elif self.actual_profile == 2 and \
                        (self.slot_position['income_price'] - self.min_max_bid(self._ipd2_s, self._ipd2_n)) > self.actual_ask_price and \
                        self.smoot_fast_price_history[-1] > self.smoot_fast_price_history[-2]:
                    self.buy()
                elif self.actual_profile == 3 and \
                        (self.slot_position['income_price'] - self.min_max_bid(self._ipd3_s, self._ipd3_n)) > self.actual_ask_price and \
                        self.smoot_fast_price_history[-1] > self.smoot_fast_price_history[-2]:
                    self.buy()

        await self.async_client_detect.close_connection()

    async def async_websocket_bookticker_stopper(self):
        i_socket_list = [self.get_socket_name(self.symbol, "bookticker")]
        # for symbol in self._traded_symbols:

        self.async_client_stopper = await AsyncClient.create()
        self.bm_stopper = BinanceSocketManager(self.async_client_stopper)
        self.ts_stopper = self.bm_stopper.multiplex_socket(i_socket_list)

        async with self.ts_stopper as tscm:
            while self.threads_in_progress:
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

        # z = np.array([self.sh_slot_position_array[:],
        #               self.sh_trade_time[:],
        #               self.sh_status_array[:],
        #               self.sh_binance_action_limit[:],
        #               self.transfer[:]], dtype=object)

        # np.savez_compressed("./sync_local/transfer_sh_slot_position_array.npz", self.sh_slot_position_array[:])
        # np.savez_compressed("./sync_local/transfer_sh_trade_time.npz", self.sh_trade_time[:])
        # np.savez_compressed("./sync_local/transfer_sh_status_array.npz", self.sh_status_array[:])
        # np.savez_compressed("./sync_local/transfer_sh_binance_action_limit.npz", self.sh_binance_action_limit[:])
        # np.savez_compressed("./sync_local/transfer_timeseries.npz", self.transfer[:])
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
        while self.threads_in_progress:
            if self.stop_flag[0] == 0:
                self.threads_in_progress = False
            time.sleep(3)
            # self.print_data()
            self.data_transfer()

        print('Monitor shutdown.')
        self.print_data()


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
    # decision_history = Queue(maxsize=time_period)

    best_bid_price_history = Array('f', [0.0] * time_period)
    best_ask_price_history = Array('f', [0.0] * time_period)
    renko_slow_price_history = Array('f', [0.0] * time_period)
    renko_fast_price_history = Array('f', [0.0] * time_period)
    renko_stop_price_history = Array('f', [0.0] * time_period)
    smoot_slow_price_history = Array('f', [0.0] * time_period)
    smoot_fast_price_history = Array('f', [0.0] * time_period)
    stop_flag = Array('i', [1])

    # 4 slow300_gap_0_0005
    # 3 slow500_gap_0_001
    # 2 slow2000_gap_0_0005
    # 1 slow1000_gap_0_001 # 3as al egyenlő de 001 a minimum mennyiség (20 dolláros)

    setting = "slow2000_gap_0_0005"
    print(setting)
    if setting == "slow1000":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000045,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow1000",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.3, .6],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "slow1000_gap":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow1000_gap",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.0006,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.6, .8],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "fast30":
        ntick = .5 / 15000
        ddown_state = 6
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 4,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000020,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 4,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000045,
                'income_price_distance': [5, 10],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 4,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [7, 10],
            },
        }

        params = {'simulation': True,
                  'name': "fast30",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 30,
                  'max_invest_quote': 220,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.3, .6],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "slow30":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000045,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow30",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 30,
                  'max_invest_quote': 220,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.3, .6],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "slow30_gap":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [2, 4],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000095,
                'income_price_distance': [3, 4],
            },
        }

        params = {'simulation': True,
                  'name': "slow30_gap",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 30,
                  'max_invest_quote': 220,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.3, .6],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "fast1000":
        ntick = .5 / 15000
        ddown_state = 6
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 15,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000005,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 15,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [5, 10],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 8,
                'max_time_sec': 60 * 15,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000045,
                'income_price_distance': [7, 10],
            },
        }

        params = {'simulation': True,
                  'name': "fast100",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.001,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.3, .6],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "slow1000_gap_0_001":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow1000_gap_0_001",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.001,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.6, .8],
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
                  'stop_flag': stop_flag,

                  }
    elif setting == "slow300_gap_0_0005":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow300_gap_0_0005",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 300,
                  'max_invest_quote': 1500,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.6, .8],
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
                  'stop_flag': stop_flag,
                  }
    elif setting == "slow500_gap_0_001":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000002,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow500_gap_0_001",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 500,
                  'max_invest_quote': 2500,
                  'minimum_buy_qty_base': 0.001,
                  'buy_multiplier': 1,
                  'trade_profile_limits': [.6, .8],
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
                  'stop_flag': stop_flag,
                  }
    elif setting == "slow2000_gap_0_0005":
        ntick = .5 / 15000
        ddown_state = 8
        ddown_multiplier = 1
        trading_profile = {
            1: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 4,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (1 * ddown_multiplier),
                'ddown_depth': -1800,
                'price_step_delta': 0.000001,
                'income_price_distance': [0, 0],
            },
            2: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 6,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (2 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000065,
                'income_price_distance': [1, 3],
            },
            3: {
                'stop_delta': ntick * 3,
                'trailer_delta': ntick * 1,
                'take_delta': ntick * 12,
                'max_time_sec': 60 * 30,
                'renko_slow_steps': 8,
                'renko_fast_steps': .5,
                'renko_stop_steps': .75,
                'ddown_limit': ddown_state * (3 * ddown_multiplier),
                'ddown_depth': -3500,
                'price_step_delta': 0.000025,
                'income_price_distance': [1, 3],
            },
        }

        params = {'simulation': True,
                  'name': "slow2000_gap_0_0005",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'trading_profile': trading_profile,
                  'deposit_quote': 2000,
                  'max_invest_quote': 10000,
                  'minimum_buy_qty_base': 0.0005,
                  'buy_multiplier': 1.1,
                  'trade_profile_limits': [.6, .8],
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
                  'stop_flag': stop_flag,

                  }

    n_acrea = AcReA
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
               'renko_slow_price_history': renko_slow_price_history,
               'renko_fast_price_history': renko_fast_price_history,
               'renko_stop_price_history': renko_stop_price_history,
               'smoot_slow_price_history': smoot_slow_price_history,
               'smoot_fast_price_history': smoot_fast_price_history,
               'decision_history': decision_history,
               'stop_flag': stop_flag,

               }

    n_monitor = Monitor
    process2 = Process(target=n_monitor, args=(params2,))
    process2.start()

    process1.join()
    process2.join()
