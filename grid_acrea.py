# import sys
import time
import os
# import pickle
from datetime import datetime
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
    # market sebeség mérő

    def __init__(self, time_period, extra_time):
        self.time_period = time_period
        self.extra_time = extra_time
        # self.socket_action = typed.Dict.empty(*time_dict_type)
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

        orders_2 = np.sum(self.order_reg[0:sec + 1]) + np.sum(self.order_reg_yesterday[86400 - (86400 - sec):-1])

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
        self.price_arr = np.append(self.price_arr, np.array([float(price)]), axis=0)
        self.qty_arr = np.append(self.qty_arr, np.array([float(qty)]), axis=0)

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

    def get_max_price(self):
        return np.max(self.price_arr, initial=0.0)

    def get_min_price(self):
        return np.min(self.price_arr, initial=10000000.0)

    def get_total_qty(self):
        v = np.sum(self.qty_arr)
        return float(v)


class Grid_AcReA:
    def __init__(self, param):
        self.name = param['name']
        self.stop_flag = param['stop_flag']
        self.back_length = param['back_length']
        self.round_block = param['round_block']

        self.sh_riport0 = param['riport0']
        self.sh_riport1 = param['riport1']
        self.sh_riport2 = param['riport2']
        self.sh_riport3 = param['riport3']
        self.sh_riport4 = param['riport4']
        self.sh_riport5 = param['riport5']
        self.sh_riport6 = param['riport6']
        self.sh_riport7 = param['riport7']
        self.sh_riport8 = param['riport8']
        self.sh_riport9 = param['riport9']

        self.trade_in_progres = True
        self.threads_in_progress = True

        self.simulation = param['simulation']

        self.status = {'profit': 0.0,
                       'turnover': 0.0,
                       'max_qty': 0.0,
                       'max_drawdown': 0.0,
                       'simulation': self.simulation,
                       }

        self.reg_trading_mode()

        self.time_period = param['time_period']
        self.sal = SocketActionLimit(time_period=5000, extra_time=0)
        self.tr = TradeRegister()
        self.max_socket_action = 0
        self.max_socket_action_dt = datetime.now()

        self.deposit_quote = param['deposit_quote']
        self.server_start_dt = datetime.now()
        time.sleep(1)  # azért, hogy elkerüljem a nullával való osztátst, mert a profitot eltelt másodpercre számolom

        self.trade_time = np.array([0] * 100, dtype=np.int32)

        self.bal = BinanceActionLimit()
        # self.binance_action_limit = param['binance_action_limit']

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
        # self.buy_multiplier = float(param['buy_multiplier'])

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

        self.slot = {
            'start_delta': [0, 0],
        }

        self.trading_profile = param['trading_profile']

        self.slot_position = {'qty': 0.0,
                              'income_price': 0.0,
                              'free_invest_quote': self.max_invest_quote,
                              'actual_pnl': 0.0,
                              'actual_profile': 0,
                              'last_buy_price': 100000000.0,
                              'exit_value': 0.0,
                              }

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

        self.sh_value_history = param['value_history']
        # kezdő értéknek megkapja a maxi invest quote-ot
        self.sh_value_history[:] = np.array([self.deposit_quote] * self.time_period)[:]

        self.decision_neutral = 0
        self.decision_long = 500
        self.decision_stop = -500
        # self.decision_buy_again = 250

        self.decision_history = np.array([0] * self.time_period, dtype=np.int32)
        self.sh_decision_history = param['decision_history']

        self.monitor_max_qty = 0.0
        self.max_drawdown = 0.0

        self.actual_profile = 0
        self.change_profile(1)

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
        self.status['simulation'] = True if self.simulation else False

    @property
    def time_delta(self):
        return int((datetime.now() - self.server_start_dt).total_seconds())

    def set_status(self, name, value, static=False):
        if static:
            self.status[name] = value
        else:
            self.status[name] += value

        # egyéb státusz elemk regisztrálása
        self.status['simulation'] = True if self.simulation else False

        # profitot számolja ki
        # self.status_array['profit'] = round(self.status_array[5] / self.time_delta * (24 * 60 * 60 * 365) / self.deposit_quote, 4)

    def change_profile(self, p):
        if p != self.actual_profile:
            self.slot['start_delta'] = self.trading_profile[p]['start_delta']
            self.actual_profile = int(p)
            self.slot_position['actual_profile'] = int(p)
            # self.set_slot_position_array()

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

    def get_actual_value(self):
        qty = round(self.slot_position['qty'], 8)
        free_invest_quote = round(self.slot_position['free_invest_quote'], 4)
        return round((self.actual_bid_price * qty) + free_invest_quote, 8)

    # Riport

    def get_riport_basic_str(self):
        simulation = 'Simulation  ' if self.simulation else ''
        r_str = f"{simulation}{self.name}   Start: {self.server_start_dt}    Symbol: {self.symbol}    Deposit: {self.deposit_quote}   Max_invest: {self.max_invest_quote}" \
                f"   Buy_qty:{self.minimum_buy_qty_base}"
        return r_str

    def get_riport_profit_str(self):
        act_value = self.get_actual_value()
        real_pnl = round(act_value - self.max_invest_quote, 8)
        real_yield = round(real_pnl / self.time_delta * (24 * 60 * 60 * 365) / self.deposit_quote, 4)
        free_invest_quote = round(self.slot_position['free_invest_quote'], 4)
        realised_profit = self.status['profit']
        r_str = f"P&L: {real_pnl}  Yield (annual): {real_yield} %   Realised P&L: {realised_profit}   Value: {act_value}   Free_invest: {free_invest_quote}"
        return r_str

    def get_riport_position_str(self):
        qty = round(self.slot_position['qty'], 8)
        actual_profile = self.slot_position['actual_profile']
        lastp = round(self.slot_position['last_buy_price'], 8)
        minp = round(self.tr.get_min_price(), 8)
        maxp = round(self.tr.get_max_price(), 8)
        r_str = f"Qty: {qty}   last_p.: {lastp}   min_p.: {minp}   max_p.: {maxp}   act._profile: {actual_profile}"
        return r_str

    def get_riport_limit_str(self):
        t = datetime.now().time()
        sec = (t.hour * 60 + t.minute) * 60 + t.second
        blocked_actions = self.bal.get_blocked_actions()
        sum_orders24 = self.bal.get_sum_orders24(sec)
        max_order = self.bal.get_max_order()
        max_request = self.bal.get_max_request()

        r_str = f"Blocked actions: {blocked_actions}     Sum_orders24 (160000 / 24h): {sum_orders24}     max_order (50 / 10 sec): {max_order}    " \
                f"Max request (1200 / 60 sec): {max_request}   "
        return r_str

    def get_riport_status_str(self):
        monitor_turnover = round(float(self.status['turnover']), 2)
        monitor_max_qty = round(float(self.status['max_qty']), 6)
        monitor_min_value = round(float(self.status['max_drawdown']), 2)
        r_str = f"Turn_over: {monitor_turnover} USD   Max.qty: {monitor_max_qty} BTC   Max.DD: {monitor_min_value} USD"
        return r_str

    def get_riport_trade_time_str(self):
        min_tr = np.min(self.trade_time[self.trade_time > 0], initial=1000000)
        max_tr = np.max(self.trade_time[self.trade_time > 0], initial=0)
        if self.trade_time[self.trade_time > 0].shape[0] > 0:
            avg_tr = np.mean(self.trade_time[self.trade_time > 0])
        else:
            avg_tr = 0
        r_str = f"Min_trade_time: {min_tr} ms   Max_trade_time: {max_tr} ms    Avg_trade_time: {avg_tr} ms"
        return r_str

    @staticmethod
    def fix_size(text, size=200):
        space = ' ' * 200
        text = text + space
        text = text[:size]
        return bytes(text, 'utf-8')

    def data_transfer(self):

        while self.threads_in_progress:
            self.sh_best_bid_price_history[:] = self.best_bid_price_history[:]
            self.sh_best_ask_price_history[:] = self.best_ask_price_history[:]
            self.sh_uniform_bid_price_history[:] = self.uniform_bid_price_history[:]
            self.sh_uniform_ask_price_history[:] = self.uniform_ask_price_history[:]
            self.sh_decision_history[:] = self.decision_history[:]
            # self.sh_trade_time[:] = self.trade_time[:]

            # a pnl rávetítem a önerőre
            own_value = round(self.get_actual_value() - self.max_invest_quote, 8) + self.deposit_quote
            self.sh_value_history[:-1] = self.sh_value_history[1:]
            self.sh_value_history[-1] = own_value

            self.sh_riport0[:] = self.fix_size(self.get_riport_basic_str())[:]
            self.sh_riport1[:] = self.fix_size(self.get_riport_profit_str())[:]
            self.sh_riport2[:] = self.fix_size(self.get_riport_position_str())[:]
            self.sh_riport3[:] = self.fix_size(self.get_riport_limit_str())[:]
            self.sh_riport4[:] = self.fix_size(self.get_riport_status_str())[:]
            self.sh_riport5[:] = self.fix_size(self.get_riport_trade_time_str())[:]

            # amennyiben az ár vétel vagy eladás nélkül emelkedik itt átállítódik
            self.set_profile()

            if os.path.exists('./stop.txt'):
                os.remove('./stop.txt')
                self.trade_in_progres = False
            else:
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

    def set_profile(self):
        # Risk management
        # az elköltött pénz arányában állítja az aktuális profilt
        position_value = self.slot_position['qty'] * self.actual_bid_price
        total_exit_value = self.slot_position['free_invest_quote'] + position_value
        # a teljes rendelkezésre álló pénz hány százalékát költöttem el
        invested_ration = position_value / total_exit_value
        i_profit = np.searchsorted(self.trade_profile_limits, invested_ration) + 1
        self.change_profile(i_profit)

    def buy(self):

        # shut down hez kell
        if not self.check_trade_right():
            # or not self.sal.is_trade_allow(int(datetime.now().timestamp())):
            return

        # első x ezer adat megérkezéséig nem kötök
        if not self.sal.is_ready_to_start():
            return

        # if self.slot_position['qty'] > 0 and self.slot_position['last_buy_price'] * (1 - self.price_step_delta) < self.actual_ask_price:
        #     return

        ask_price_fixed = self.actual_ask_price

        # Risk management
        if self.tr.get_total_qty() > 0:
            if self.actual_profile == 1:
                if ask_price_fixed < self.tr.get_max_price() - self.slot['start_delta'][1]:
                    return
            else:
                if self.tr.get_max_price() - self.slot['start_delta'][0] < ask_price_fixed or\
                        self.tr.get_max_price() - self.slot['start_delta'][1] > ask_price_fixed:
                    return

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

            # existed_value = self.slot_position['income_price'] * self.slot_position['qty']
            # existed_value = self.slot_position['income_price'] * self.slot_position['qty']
            # new_value = calc_qty * ask_price_fixed
            # new_value = cummulative_quote_qty

            # self.slot_position['qty'] += calc_qty  # new qty
            self.slot_position['qty'] = self.tr.get_total_qty()  # new qty
            # print("self.slot_position['qty']", self.slot_position['qty'])
            self.slot_position['free_invest_quote'] -= cummulative_quote_qty
            self.monitor_max_qty = np.max([self.monitor_max_qty, self.slot_position['qty']])
            self.set_status('max_qty', self.monitor_max_qty, static=True)
            # self.status('turnover', calc_qty * ask_price_fixed)
            self.set_status('turnover', cummulative_quote_qty)
            # self.slot_position['income_price'] = (existed_value + new_value) / self.slot_position['qty']
            self.slot_position['income_price'] = self.tr.get_open_position_income_price()
            # self.slot_position['last_buy_price'] = ask_price_fixed
            self.slot_position['last_buy_price'] = traded_price

            # Risk management
            self.set_profile()

            self.decision_history[-1] = self.decision_long
            self.reg_order()

    def stop(self, message=""):
        # if self.slot_position['income_price'] > self.actual_bid_price or message != "Time":
        fix_bid_price = self.actual_bid_price
        sell_qty, income_value = self.tr.get_qty(fix_bid_price, spread=0)
        if sell_qty > 0:
            executed_qty, cummulative_quote_qty, traded_price = self.order_sell(sell_qty)
            self.tr.remove_qty(fix_bid_price, spread=0)

            # income_value = self.slot_position['qty'] * self.slot_position['income_price']
            # exit_value = self.slot_position['qty'] * self.actual_bid_price
            exit_value = cummulative_quote_qty
            self.set_status('profit', exit_value - income_value)
            self.slot_position['free_invest_quote'] += exit_value
            self.slot_position['qty'] = self.tr.get_total_qty()
            self.slot_position['income_price'] = self.tr.get_open_position_income_price()
            self.set_status('turnover', exit_value)
            self.decision_history[-1] = self.decision_stop
            self.actual_buy_qty_base = self.start_buy_qty_base
            self.set_profile()
            # self.set_profile(1, direct=True)
            # self.set_slot_position_array()
            self.reg_order()
            # self.m_flag = False
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
                # self.socket_counter += 1
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
                               self.add_decision_history())

                asyncio.gather(self.add_bid_uniform_filter(),
                               self.add_ask_uniform_filter())

                # if not self.sal.is_trade_allow(int(datetime.now().timestamp())):
                #     continue

                if self.uniform_ask_price_history[-2] < self.uniform_ask_price_history[-1] and self.sal.is_ready_to_start():
                    # print('------------------------------------------------------')
                    b_value, b_length = self.back_signal(self.uniform_ask_price_history[-3000:-1])
                    # print('fel', datetime.now().second, b_value, b_length)
                    # print(self.uniform_ask_price_history[-100:])

                    if b_value > self.uniform_ask_price_history[-2] and b_length > self.back_length:
                        # print('buy', datetime.now().second)
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

                # if not self.sal.is_trade_allow(int(datetime.now().timestamp())) and self.slot_position['qty'] > 0:
                #     self.stop("Stop")
                #     self.status('stop', 1)

                # self.actual_ask_qty[symbol] = float(res['data']['A'])

                # if self.renko_stop_steps <= abs(self.renko_stop_price_history[-1] - bid):
                #     self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [bid], axis=0), 0)
                # else:
                #     self.renko_stop_price_history = np.delete(np.append(self.renko_stop_price_history, [self.renko_stop_price_history[-1]], axis=0), 0)

                self.slot_position['actual_pnl'] = self.slot_position['qty'] * (self.actual_bid_price - self.slot_position['income_price'])
                # self.sh_slot_position_array[7] = self.slot_position['actual_pnl']
                self.slot_position['exit_value'] = self.slot_position['free_invest_quote'] + (self.slot_position['qty'] * self.actual_bid_price)

                # print(self.slot_position_array)
                self.max_drawdown = np.min([self.max_drawdown, self.slot_position['actual_pnl']])
                self.set_status('max_drawdown', self.max_drawdown, static=True)
                # Stopper

                # selected_price = self.renko_stop_price_history[-1]
                # selected_price = self.actual_bid_price

                if self.uniform_bid_price_history[-2] > self.uniform_bid_price_history[-1] and self.sal.is_ready_to_start():
                    # print("sell gap")
                    b_value, b_length = self.back_signal(self.uniform_bid_price_history[-3000:-1])
                    if b_value < self.uniform_ask_price_history[-2] and b_length > self.back_length:
                        # print('sell', datetime.now().second)
                        self.stop()

        await self.async_client_stopper.close_connection()


class Monitor:

    def __init__(self, param):
        self.start_dt = datetime.now()
        self.threads_in_progress = True
        self.stop_flag = param['stop_flag']
        self.name = param['name']

        self.sh_best_bid_price_history = param['best_bid_price_history']
        self.sh_best_ask_price_history = param['best_ask_price_history']
        self.sh_uniform_bid_price_history = param['uniform_bid_price_history']
        self.sh_uniform_ask_price_history = param['uniform_ask_price_history']
        self.sh_decision_history = param['decision_history']
        self.sh_value_history = param['value_history']

        self.sh_riport0 = param['riport0']
        self.sh_riport1 = param['riport1']
        self.sh_riport2 = param['riport2']
        self.sh_riport3 = param['riport3']
        self.sh_riport4 = param['riport4']
        self.sh_riport5 = param['riport5']
        self.sh_riport6 = param['riport6']
        self.sh_riport7 = param['riport7']
        self.sh_riport8 = param['riport8']
        self.sh_riport9 = param['riport9']

        self.riports = []

        self.time_period = param['time_period']

        self.transfer = np.array([[0.0] * self.time_period] * 8)

        self.process = param['process']
        self.cores = param['cores']
        self.mpi = str(self.process) + "/" + str(self.cores) + " core ->"
        self.data_manager()

    def data_transfer(self):

        self.transfer[0][:] = np.array(self.sh_best_bid_price_history[:])
        self.transfer[1][:] = np.array(self.sh_best_ask_price_history[:])
        self.transfer[2][:] = np.array(self.sh_uniform_bid_price_history[:])
        self.transfer[3][:] = np.array(self.sh_uniform_ask_price_history[:])
        self.transfer[4][:] = np.array(self.sh_decision_history[:])
        self.transfer[5][:] = np.array(self.sh_value_history[:])

        self.riports = [
            self.sh_riport0[:],
            self.sh_riport1[:],
            self.sh_riport2[:],
            self.sh_riport3[:],
            self.sh_riport4[:],
            self.sh_riport5[:],
            self.sh_riport6[:],
            self.sh_riport7[:],
            self.sh_riport8[:],
            self.sh_riport9[:],
        ]

        np.savez_compressed("./sync_local/transfer_all.npz",
                            array1=self.transfer,
                            array2=self.riports,
                            )

    def data_manager(self):
        while self.threads_in_progress:
            if self.stop_flag[0] == 0:
                self.threads_in_progress = False
            time.sleep(3)
            # self.print_data()
            self.data_transfer()

        print('Monitor shutdown.')


# __main__
if __name__ == '__main__':
    path = "./venv/"
    if os.path.exists(path):
        print('Local running?')
        # sys.exit()

    cores = cpu_count()
    running_processes = []

    ###################
    #  shared memory  #
    ###################

    time_period = 20000

    riport_len = 200
    riport0 = Array('c', b'0' * riport_len)
    riport1 = Array('c', b'0' * riport_len)
    riport2 = Array('c', b'0' * riport_len)
    riport3 = Array('c', b'0' * riport_len)
    riport4 = Array('c', b'0' * riport_len)
    riport5 = Array('c', b'0' * riport_len)
    riport6 = Array('c', b'0' * riport_len)
    riport7 = Array('c', b'0' * riport_len)
    riport8 = Array('c', b'0' * riport_len)
    riport9 = Array('c', b'0' * riport_len)

    best_bid_price_history = Array('f', [0.0] * time_period)
    best_ask_price_history = Array('f', [0.0] * time_period)
    uniform_bid_price_history = Array('f', [0.0] * time_period)
    uniform_ask_price_history = Array('f', [0.0] * time_period)
    decision_history = Array('i', [0] * time_period)
    value_history = Array('f', [0.0] * time_period)

    stop_flag = Array('i', [1])

    # 4 slow2000_.003_1_220_60*6 # egyből zát ha 220 felé megy
    # 3 slow30 270/60 220/120 tiltás
    # 2 slow2000_gap_0_0012_kaiser_125 275/60 * 30 tiltás
    # 1 slow2000_gap_0_0012_kaiser_125 220/120 tiltás

    setting = "slow1000"
    print(setting)
    params = {}
    if setting == "slow1000":
        trading_profile = {
            1: {
                'start_delta': [0, 500],  # BUSD
            },
            2: {
                'start_delta': [500, 1000],  # BUSD
            },
            3: {
                'start_delta': [1000, 1500],  # BUSD
            },
        }

        params = {'simulation': True,
                  'name': "slow1000",
                  'cores': cores,
                  'process': 1,
                  'base': "BTC",
                  'quote': "BUSD",
                  'deposit_quote': 1000,
                  'max_invest_quote': 4000,
                  'minimum_buy_qty_base': 0.001,
                  'trade_profile_limits': [.33, .66],
                  'trading_profile': trading_profile,
                  'back_length': 25,
                  'round_block': 1.25,
                  # 'buy_multiplier': 1,
                  'time_period': time_period,
                  'best_bid_price_history': best_bid_price_history,
                  'best_ask_price_history': best_ask_price_history,
                  'uniform_bid_price_history': uniform_bid_price_history,
                  'uniform_ask_price_history': uniform_ask_price_history,
                  'decision_history': decision_history,
                  'value_history': value_history,
                  'stop_flag': stop_flag,
                  'riport0': riport0,
                  'riport1': riport1,
                  'riport2': riport2,
                  'riport3': riport3,
                  'riport4': riport4,
                  'riport5': riport5,
                  'riport6': riport6,
                  'riport7': riport7,
                  'riport8': riport8,
                  'riport9': riport9,

                  }

    n_acrea = Grid_AcReA
    process1 = Process(target=n_acrea, args=(params,))
    process1.start()

    params2 = {'cores': cores,
               'name': params['name'],
               'process': 2,
               'base': "BTC",
               'quote': "USDT",
               'time_period': time_period,
               'best_bid_price_history': best_bid_price_history,
               'best_ask_price_history': best_ask_price_history,
               'uniform_bid_price_history': uniform_bid_price_history,
               'uniform_ask_price_history': uniform_ask_price_history,
               'decision_history': decision_history,
               'value_history': value_history,
               'stop_flag': stop_flag,
               'riport0': riport0,
               'riport1': riport1,
               'riport2': riport2,
               'riport3': riport3,
               'riport4': riport4,
               'riport5': riport5,
               'riport6': riport6,
               'riport7': riport7,
               'riport8': riport8,
               'riport9': riport9,

               }

    n_monitor = Monitor
    process2 = Process(target=n_monitor, args=(params2,))
    process2.start()

    process1.join()
    process2.join()
