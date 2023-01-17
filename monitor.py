import os
import sys
import time
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import seaborn as sns
from scipy.signal import savgol_filter
import asyncio
from threading import Thread

np.set_printoptions(threshold=5000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', '{:8,.4f}'.format)
pd.set_option('display.max_colwidth', None)
from n_trade_server_connection import n_trade_server_connection



class LOBMonitor:

    def __init__(self):
        self.tc = n_trade_server_connection()

        self.server = 1
        if self.server == 1:
            self.sever_name = "AcReA"
            self.tc.password = '2jQ._n]?}+C=-kAs'
            self.tc.hostname = '108.61.200.183'
        else:
            self.sever_name = "LOB2"
            self.tc.password = '6.Zz_.c[y9!PPydr'
            self.tc.hostname = '108.61.181.128'

        self.zoom_part = -3000
        self.time_period = 20000

        self.best_bid_price_history = np.array([0.0] * self.time_period)
        self.best_ask_price_history = np.array([0.0] * self.time_period)
        self.renko_slow_price_history = np.array([0.0] * self.time_period)
        self.renko_fast_price_history = np.array([0.0] * self.time_period)
        self.renko_stop_price_history = np.array([0.0] * self.time_period)
        self.smoot_slow_price_history = np.array([0.0] * self.time_period)
        self.smoot_fast_price_history = np.array([0.0] * self.time_period)
        self.decision_history = np.array([0.0] * self.time_period)

        self.last_best_bid_price_history = 0.0

        self.binance_action_limit = {}
        self.trade_time = {}
        self.slot_position = {}
        self.status = {}

        self.ready_to_plot = False
        # self.transfer_position = {}
        # self.transfer_statistics = {}


        # plot
        self.ylim_max = 0
        self.ylim_min = 0
        self.xaxis = np.arange(0, self.time_period)
        self.xaxis = self.xaxis.reshape((-1, 1))
        self.xaxis_zoom = np.arange(0, abs(self.zoom_part))
        self.xaxis_zoom = self.xaxis_zoom.reshape((-1, 1))

        self.trend = ""
        self.last_traded_price = 0
        self.c = 0

        self.project_name = 'LOB'
        self.models = {}
        # self.build_ai()

    def strat_threads(self):
        task1 = Thread(target=self.data_transfer, args=[])
        task1.start()

        while not self.ready_to_plot:
            time.sleep(1)



        # Data plotter


        # self.best_mid_price_history[self.best_mid_price_history == 0] = self.best_mid_price_history[-1]
        # self.best_bid_price_history[self.best_bid_price_history == 0] = self.best_bid_price_history[-1]
        # self.best_ask_price_history[self.best_ask_price_history == 0] = self.best_ask_price_history[-1]
        # self.best_smoot_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.best_smoot2_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.best_smoot3_price_history = np.full(self.time_period, self.best_mid_price_history[-1])
        # self.market_speed_array[self.market_speed_array > 5] = 0
        #
        #

        # fm = plt.get_current_fig_manager()
        # fm.window.setGeometry = (0, 0, 2048, 768)

        sns.set_theme(style="whitegrid", font_scale=.8)
        self.fig, ax = plt.subplots(4, 1,
                               gridspec_kw={'height_ratios': [2, 4, 1, .5]},
                               figsize=(17, 6))

        self.ax11 = self.fig.add_subplot(4, 1, 1)
        self.ax12 = self.fig.add_subplot(4, 1, 2)
        self.ax13 = self.fig.add_subplot(4, 1, 3)
        self.ax14 = self.fig.add_subplot(4, 1, 4)

        # self.ax21 = fig.add_subplot(3, 2, 2)
        # self.ax22 = fig.add_subplot(4, 2, 4)
        # self.ax22.axis('off')
        # self.ax23 = fig.add_subplot(4, 2, 6)
        # self.ax23.axis('off')

        plt.autoscale(False)
        for axx in ax:
            axx.set_xticks([])
            axx.set_yticks([])
            # axx[0].set_xticks([])
            # axx[0].set_yticks([])
            # axx[1].set_xticks([])
            # axx[1].set_yticks([])

        plt.subplots_adjust(left=0.05, right=.98, top=.95, bottom=0.05, hspace=-0.01, wspace=0.01)
        ani = animation.FuncAnimation(self.fig, self.animate_plot, interval=1000 * 1)
        self.fig.canvas.manager.window.wm_geometry(("+0+500"))

        plt.show()

    def data_transfer(self):
        self.tc.open_connect()
        while True:
            if not self.ready_to_plot:
                self.tc.get("/root/transfer_timeseries.npy", "./monitor_data/transfer_timeseries.npy")
                self.tc.get("/root/transfer_sh_slot_position_array.npy", "./monitor_data/transfer_sh_slot_position_array.npy")
                self.tc.get("/root/transfer_sh_trade_time.npy", "./monitor_data/transfer_sh_trade_time.npy")
                self.tc.get("/root/transfer_sh_status_array.npy", "./monitor_data/transfer_sh_status_array.npy")
                self.tc.get("/root/transfer_sh_binance_action_limit.npy", "./monitor_data/transfer_sh_binance_action_limit.npy")
                try:
                    transfer = np.load("./monitor_data/transfer_timeseries.npy")

                    self.best_bid_price_history[:] = transfer[0][:]
                    self.best_ask_price_history[:] = transfer[1][:]
                    self.renko_slow_price_history[:] = transfer[2][:]
                    self.renko_fast_price_history[:] = transfer[3][:]
                    self.renko_stop_price_history[:] = transfer[4][:]
                    self.smoot_slow_price_history[:] = transfer[5][:]
                    self.smoot_fast_price_history[:] = transfer[6][:]
                    self.decision_history[:] = transfer[7][:]

                    binance_action_limit_array = np.load("./monitor_data/transfer_sh_binance_action_limit.npy")

                    self.binance_action_limit = {
                        "blocked_actions": binance_action_limit_array[0],
                        "sum_orders24": binance_action_limit_array[1],
                        "max_order": binance_action_limit_array[2],
                        "max_request": binance_action_limit_array[3],
                    }

                    trade_time_array = np.load("./monitor_data/transfer_sh_trade_time.npy")

                    self.trade_time = {
                        "trade_time_min": np.min(trade_time_array),
                        "trade_time_max": np.max(trade_time_array),
                        "trade_time_mean": np.mean(trade_time_array),
                    }

                    slot_position_array = np.load("./monitor_data/transfer_sh_slot_position_array.npy")

                    self.slot_position = {
                        "qty": slot_position_array[0],
                        "income_price": slot_position_array[1],
                        "free_invest_quote": slot_position_array[2],
                        "stop_price": slot_position_array[3],
                        "trailer_stop_price": slot_position_array[4],
                        "trailer_minimum_price": slot_position_array[5],
                        "take_price": slot_position_array[6],
                        "actual_value": slot_position_array[7],
                        "actual_profile": slot_position_array[8],
                        "last_buy_price": slot_position_array[9],
                    }

                    status_array = np.load("./monitor_data/transfer_sh_status_array.npy")

                    self.status = {
                        "stop": status_array[0],
                        "buy_again": status_array[1],
                        "take": status_array[2],
                        "trailer": status_array[3],
                        "time": status_array[4],
                        "profit": status_array[5],
                        "turnover": status_array[6],
                        "max_qty": status_array[7],
                        "min_value": status_array[8],
                    }

                    self.ready_to_plot = True
                except:
                    pass
            time.sleep(7)

    def get_satistics_str(self):
        monitor_stop = int(self.status['stop'])
        monitor_take = int(self.status['take'])
        monitor_trailer = int(self.status['trailer'])
        monitor_buy_again = int(self.status['buy_again'])
        monitor_time = int(self.status['time'])
        monitor_profit = round(float(self.status['profit']), 2)
        monitor_turnover = round(float(self.status['turnover']), 2)
        monitor_max_qty = round(float(self.status['max_qty']), 2)
        monitor_min_value = round(float(self.status['min_value']), 2)
        r_str = f"Take:{monitor_take}   Stop:{monitor_stop}   Trailer:{monitor_trailer}  " \
                f"Buy_again:{monitor_buy_again}   Time:{monitor_time}   Profit:{monitor_profit} USD     " \
                f"Turn_over:{monitor_turnover} USD      Max.qty:{monitor_max_qty} BTC      Min.value:{monitor_min_value} USD"
        return r_str

    def get_title_str(self):

        qty = round(self.slot_position['qty'], 8)
        free_invest_quote = round(self.slot_position['free_invest_quote'], 4)
        actual_value = round(self.slot_position['actual_value'], 4)
        actual_profile = round(self.slot_position['actual_profile'], 4)
        last_buy_price = self.slot_position['last_buy_price']

        r_str = f"{self.sever_name}        Actual position:      qty:{qty} BTC     free_invest:{free_invest_quote} USD     " \
                f"actual_value{actual_value} USD     last_buy_price:{last_buy_price} USD     actual_profile{actual_profile} "
        return r_str

    def animate_plot(self, i):

        if self.ready_to_plot and self.last_best_bid_price_history != np.sum(self.best_bid_price_history):
            self.last_best_bid_price_history = np.sum(self.best_bid_price_history)

            self.fig.canvas.manager.set_window_title(self.get_title_str())

            self.ax11.clear()
            self.ax11.margins(x=0)
            self.ylim_min = np.min(self.best_bid_price_history[self.best_bid_price_history > 0]) - .5
            self.ylim_max = np.max(self.best_ask_price_history[self.best_ask_price_history > 0]) + .5

            self.ax11.set_ylim([self.ylim_min, self.ylim_max])
            self.ax11.ticklabel_format(axis='y', style='sci', useOffset=False)
            self.ax11.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax11.set_facecolor('#efefef')

            self.ax11.plot(self.xaxis, self.smoot_slow_price_history[:], 'k-', linewidth=2)
            self.ax11.plot(self.xaxis, self.smoot_fast_price_history[:], 'k--', linewidth=1)
            self.ax11.plot(self.xaxis, self.best_ask_price_history[:], 'b-', alpha=0.6, linewidth=1)
            self.ax11.plot(self.xaxis, self.best_bid_price_history[:], 'r-', alpha=0.6, linewidth=1, )
            # if self.transfer_status['income_price'] != 0:
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['income_price_short']), 'b-', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['stop_price']), 'r-', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['take_price']), 'y-', alpha=1, linewidth=2, )

            self.ax11.legend(['rs slow', 'rs fast', 'ask', 'bid'], loc=2)

            # ZOOM

            self.ax12.clear()
            self.ax12.margins(x=0)
            if self.slot_position['income_price'] != 0:
                self.ylim_min = np.min(self.best_bid_price_history[:][self.zoom_part:][self.best_bid_price_history[:][self.zoom_part:] > 0]) - .5
                self.ylim_max = np.max(self.best_ask_price_history[:][self.zoom_part:][self.best_ask_price_history[:][self.zoom_part:] > 0]) + .5

                self.ylim_min = np.min([self.ylim_min, self.slot_position['stop_price']]) - .5
                self.ylim_max = np.max([self.ylim_max, self.slot_position['take_price']]) + .5
            else:
                self.ylim_min = np.min(self.best_bid_price_history[self.zoom_part:][self.best_bid_price_history[self.zoom_part:] > 0]) - .5
                self.ylim_max = np.max(self.best_ask_price_history[self.zoom_part:][self.best_ask_price_history[self.zoom_part:] > 0]) + .5

            self.ax12.set_ylim([self.ylim_min, self.ylim_max])
            self.ax12.ticklabel_format(axis='y', style='sci', useOffset=False)
            self.ax12.xaxis.set_ticks(np.arange(0, abs(self.zoom_part), 500))

            self.ax12.plot(self.xaxis_zoom, self.smoot_slow_price_history[self.zoom_part:], 'k-', linewidth=2)
            self.ax12.plot(self.xaxis_zoom, self.smoot_fast_price_history[self.zoom_part:], 'k--', linewidth=1)
            self.ax12.plot(self.xaxis_zoom, self.best_ask_price_history[self.zoom_part:], 'b-', alpha=0.6, linewidth=1)
            self.ax12.plot(self.xaxis_zoom, self.best_bid_price_history[self.zoom_part:], 'r-', alpha=0.6, linewidth=1, )
            if self.slot_position['income_price'] != 0:
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.slot_position['income_price']), 'b-', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.slot_position['stop_price']), 'r-', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.slot_position['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.slot_position['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.slot_position['take_price']), 'y-', alpha=1, linewidth=2, )

            self.ax12.legend(['rs slow', 'rs fast', 'ask', 'bid'], loc=2)


            # # print(self.ylim_min, self.ylim_max)
            # # self.ax1.plot(x, y_bid)
            # self.ax12.clear()
            # self.ax12.margins(x=0.01)
            # self.ax12.get_yaxis().set_ticks([])
            #
            # self.market_speed_array[self.market_speed_array > 400] = 400
            # self.market_speed_array_avg[self.market_speed_array_avg > 400] = 400
            # # avg1 = np.mean(self.market_speed_array)
            # self.market_speed_array[self.market_speed_array == 0] = np.min(self.market_speed_array[self.market_speed_array > 0])
            # # avg2 = np.mean(self.market_speed_array_avg)
            # self.market_speed_array_avg[self.market_speed_array_avg == 0] = np.min(self.market_speed_array_avg[self.market_speed_array_avg > 0])


            # self.ax12.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.ax12.plot(self.xaxis, self.market_speed_array, 'y-', alpha=0.9, linewidth=2)
            # self.ax12.plot(self.xaxis, self.market_speed_array_avg, 'r-', alpha=0.6, linewidth=2)
            # self.ax12.legend(['speed', 'speed_avg'], loc=2)

            # self.ax13.clear()
            # self.ax13.margins(x=0.01)
            # self.ax13.get_yaxis().set_ticks([])
            #
            # self.ax13.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.ax13.plot(self.xaxis, self.best_ask_qty_history, 'b-', alpha=0.7, linewidth=2)
            # self.ax13.plot(self.xaxis, self.best_bid_qty_history, 'r-', alpha=0.7, linewidth=2)
            # self.ax13.legend(['ask qty', 'bid qty'], loc=2)

            # self.ax14.clear()
            # self.ax14.margins(x=0.01)
            # self.ax14.get_yaxis().set_ticks([])
            #
            # self.ax14.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.ax14.plot(self.xaxis, self.bid_ask_spread, 'y-', alpha=0.9, linewidth=2)
            # self.ax14.plot(self.xaxis, self.bid_ask_spread_avg, 'g-', alpha=0.8, linewidth=2)
            # self.ax14.legend(['b/a spread', 'b/a spread_avg'], loc=2)

            self.ax13.clear()
            self.ax13.margins(x=0)
            self.ax13.set_ylim(-500, 500)
            self.ax13.get_yaxis().set_ticks([])
            self.ax13.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax13.set_facecolor('#efefef')
            self.ax13.plot(self.xaxis_zoom, self.decision_history[self.zoom_part:], 'g-', linewidth=2)

            self.ax14.clear()
            self.ax14.grid(color='#666666', linestyle='', linewidth=0)
            self.ax14.set_facecolor('#666666')
            # self.ax14.margins(x=0)
            # self.ax14.set_ylim(100, 800)
            self.ax14.get_yaxis().set_ticks([])
            # self.ax14.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            satistics_str = self.get_satistics_str()
            self.ax14.text(0.01, 0.3, satistics_str, style='italic', fontsize=10, color="#ffffff")

            # self.ax16.clear()
            # self.ax16.margins(x=0.01)
            # self.ax16.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.ax16.set_ylim([-5000, +5000])
            # self.ax16.plot(self.xaxis, self.qty_way_history)

            # self.ax21.clear()
            # self.ax21.set_ylim([self.ylim_min, self.ylim_max])
            # self.ax21.get_yaxis().set_ticks([])
            # self.ax21.ticklabel_format(axis='y', style='plain', useOffset=False)
            # h = self.smoot_price_history
            # self.ax21.hist(h,
            #                bins='auto',
            #                range=None,
            #                weights=None,
            #                cumulative=False,
            #                bottom=None,
            #                histtype=u'bar',
            #                align=u'mid',
            #                orientation=u'horizontal',
            #                rwidth=None,
            #                log=False,
            #                color=None,
            #                label=None,
            #                stacked=False)

            self.ready_to_plot = False


if __name__ == '__main__':
    n_tob = LOBMonitor()
    n_tob.strat_threads()
    while True:
        time.sleep(50)

