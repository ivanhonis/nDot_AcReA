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

        self.zoom_part = -1000
        self.time_period = 20000
        self.best_bid_price_history = np.array([0.0] * self.time_period)
        self.last_best_bid_price_history = 0
        self.best_ask_price_history = np.array([0.0] * self.time_period)
        self.bid_ask_spread = np.array([0.0] * self.time_period)
        self.bid_ask_spread_avg = np.array([0.0] * self.time_period)
        self.best_mid_price_history = np.array([0.0] * self.time_period)
        self.best_mid_price_history_ma_fast = np.array([0.0] * self.time_period)
        self.best_mid_price_history_ma_slow = np.array([0.0] * self.time_period)
        self.smoot_price_history = np.array([0.0] * self.time_period)
        self.best_smoot2_price_history = np.array([0.0] * self.time_period)
        self.best_smoot3_price_history = np.array([0.0] * self.time_period)
        self.best_bid_qty_history = np.array([0.0] * self.time_period)
        self.best_ask_qty_history = np.array([0.0] * self.time_period)
        # self.qty_way_history = np.array([0.0] * self.time_period)
        self.traded_price_history = [0.0] * self.time_period
        self.decision_neutral = 0
        self.decision_history = [self.decision_neutral] * self.time_period
        self.market_speed_array = np.array([0.0] * self.time_period)
        self.market_speed_array_avg = np.array([0.0] * self.time_period)
        self.market_speed_stamp = datetime.now()
        self.market_speed = 2
        self.ready_to_plot = False
        self.transfer_position = {}
        self.transfer_statistics = {}

        # plot
        self.ylim_max = 0
        self.ylim_min = 0
        self.xaxis = np.arange(0, self.best_mid_price_history.shape[0])
        self.xaxis = self.xaxis.reshape((-1, 1))
        self.xaxis_zoom = np.arange(0, abs(self.zoom_part))
        self.xaxis_zomm = self.xaxis_zoom.reshape((-1, 1))

        self.trend = ""
        self.last_traded_price = 0
        self.c = 0

        self.project_name = 'LOB'
        self.models = {}
        # self.build_ai()

    def strat_threads(self):
        self.tc.open_connect()
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
        while True:
            if not self.ready_to_plot:
                self.tc.get("/root/transfer_timeseries.npy", "./monitor_data/transfer_timeseries.npy")
                self.tc.get("/root/transfer_position.pkl", "./monitor_data/transfer_position.pkl")
                self.tc.get("/root/transfer_statistics.pkl", "./monitor_data/transfer_statistics.pkl")
                try:
                    trsf = np.load("./monitor_data/transfer_timeseries.npy")

                    self.best_bid_price_history = trsf[0]
                    self.best_ask_price_history = trsf[1]
                    self.smoot_price_history = trsf[2]
                    self.decision_history = trsf[3]
                    # self.market_speed_array_avg = trsf[4]
                    # self.market_speed_array = trsf[5]
                    # self.bid_ask_spread_avg = trsf[6]
                    # self.bid_ask_spread = trsf[7]
                    # self.best_ask_qty_history = trsf[8]
                    # self.best_bid_qty_history = trsf[9]
                    self.smoot_fast_price_history = trsf[10]

                    with open('./monitor_data/transfer_position.pkl', 'rb') as handle:
                        self.transfer_position = pickle.load(handle)
                        self.transfer_position = self.transfer_position['BTCUSDT_SX3']

                    with open('./monitor_data/transfer_statistics.pkl', 'rb') as handle:
                        self.transfer_statistics = pickle.load(handle)

                    self.ready_to_plot = True
                except:
                    pass
            time.sleep(7)

    def get_satistics_str(self, status):
        def r(v):
            return round(v, 4)

        monitor_stop = int(status['monitor_stop']['BTCUSDT_SX3'])
        monitor_take = int(status['monitor_take']['BTCUSDT_SX3'])
        monitor_trailer = int(status['monitor_trailer']['BTCUSDT_SX3'])
        monitor_re_buy = int(status['monitor_re_buy']['BTCUSDT_SX3'])
        monitor_time = int(status['monitor_time']['BTCUSDT_SX3'])
        monitor_profit = status['monitor_profit']['BTCUSDT_SX3']
        monitor_fee = status['monitor_fee']['BTCUSDT_SX3']
        monitor_max_qty = status['monitor_max_qty']['BTCUSDT_SX3']
        monitor_min_value = status['monitor_min_value']['BTCUSDT_SX3']
        r_str = f"Stop:{monitor_stop}   Take:{monitor_take}   Stop:{monitor_stop}   Trailer:{monitor_trailer}  " \
              f"Re_buy:{monitor_re_buy}   Time:{monitor_time}   Profit:{r(monitor_profit)} USD     " \
              f"Fee:{r(monitor_fee)} USD      Max.qty:{r(monitor_max_qty)} BTC      Min.value:{r(monitor_min_value)} USD"
        return r_str

    def get_title_str(self):
        def r(v):
            return round(v, 4)

        symbol = self.transfer_position['symbol']
        qty = round(self.transfer_position['qty'], 8)
        income_price = round(self.transfer_position['income_price'], 4)
        stop_price = round(self.transfer_position['stop_price'], 4)
        actual_value = round(self.transfer_position['actual_value'], 4)
        actual_profile = self.transfer_position['actual_profile']

        r_str = f"Actual position:      Symbol:{symbol}   qty:{qty} BTC     income_price:{income_price} USD     " \
                f"stop_price:{stop_price} USD     actual_value{actual_value} USD    actual_profile{actual_profile} "
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

            self.ax11.plot(self.xaxis, self.smoot_price_history, 'k-', linewidth=2)
            self.ax11.plot(self.xaxis, self.smoot_fast_price_history, 'k--', linewidth=1)
            self.ax11.plot(self.xaxis, self.best_ask_price_history, 'b-', alpha=0.6, linewidth=1)
            self.ax11.plot(self.xaxis, self.best_bid_price_history, 'r-', alpha=0.6, linewidth=1, )
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
            if self.transfer_position['income_price'] != 0:
                self.ylim_min = np.min(self.best_bid_price_history[self.zoom_part:][self.best_bid_price_history[self.zoom_part:] > 0]) - .5
                self.ylim_max = np.max(self.best_ask_price_history[self.zoom_part:][self.best_ask_price_history[self.zoom_part:] > 0]) + .5

                self.ylim_min = np.min([self.ylim_min, self.transfer_position['stop_price']]) - .5
                self.ylim_max = np.max([self.ylim_max, self.transfer_position['take_price']]) + .5
            else:
                self.ylim_min = np.min(self.best_bid_price_history[self.zoom_part:][self.best_bid_price_history[self.zoom_part:] > 0]) - .5
                self.ylim_max = np.max(self.best_ask_price_history[self.zoom_part:][self.best_ask_price_history[self.zoom_part:] > 0]) + .5

            self.ax12.set_ylim([self.ylim_min, self.ylim_max])
            self.ax12.ticklabel_format(axis='y', style='sci', useOffset=False)
            self.ax12.xaxis.set_ticks(np.arange(0, abs(self.zoom_part), 500))

            self.ax12.plot(self.xaxis_zoom, self.smoot_price_history[self.zoom_part:], 'k-', linewidth=2)
            self.ax12.plot(self.xaxis_zoom, self.smoot_fast_price_history[self.zoom_part:], 'k--', linewidth=1)
            self.ax12.plot(self.xaxis_zoom, self.best_ask_price_history[self.zoom_part:], 'b-', alpha=0.6, linewidth=1)
            self.ax12.plot(self.xaxis_zoom, self.best_bid_price_history[self.zoom_part:], 'r-', alpha=0.6, linewidth=1, )
            if self.transfer_position['income_price'] != 0:
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.transfer_position['income_price']), 'b-', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.transfer_position['stop_price']), 'r-', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.transfer_position['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.transfer_position['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
                self.ax12.plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.transfer_position['take_price']), 'y-', alpha=1, linewidth=2, )

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
            satistics_str = self.get_satistics_str(self.transfer_statistics)
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

