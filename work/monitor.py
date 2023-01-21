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
sns.set_theme(style="whitegrid")
import asyncio
from threading import Thread

np.set_printoptions(threshold=5000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', '{:8,.4f}'.format)
pd.set_option('display.max_colwidth', None)
from server_connection import server_connection



class LOBMonitor:

    def __init__(self):
        self.tc = server_connection()

        self.last = -20000
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
        self.decision_neutral = 500
        self.decision_long = 600
        self.decision_short = 400
        self.decision_history = [self.decision_neutral] * self.time_period
        self.market_speed_array = np.array([0.0] * self.time_period)
        self.market_speed_array_avg = np.array([0.0] * self.time_period)
        self.market_speed_stamp = datetime.now()
        self.market_speed = 2
        self.ready_to_plot = False
        self.transfer_status = {}

        # plot
        self.ylim_max = 0
        self.ylim_min = 0
        self.xaxis = np.arange(0, self.best_mid_price_history.shape[0])
        self.xaxis = self.xaxis.reshape((-1, 1))

        self.trend = ""
        self.last_traded_price = 0
        self.c = 0

        self.project_name = 'LOB'
        self.models = {}
        # self.build_ai()

    def strat_threads(self):
        self.tc.open_connect()
        task1 = Thread(target=self.print_monitor, args=[])
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


        fig, ax = plt.subplots(5, 2,
                               gridspec_kw={'height_ratios': [6, 1, 1, 1, 1], 'width_ratios': [6, 1]},
                               figsize=(17, 6))

        self.ax11 = fig.add_subplot(5, 2, 1)
        self.ax12 = fig.add_subplot(5, 2, 3)
        self.ax13 = fig.add_subplot(5, 2, 5)
        self.ax14 = fig.add_subplot(5, 2, 7)
        self.ax15 = fig.add_subplot(5, 2, 9)
        # self.ax16 = fig.add_subplot(6, 2, 11)

        self.ax21 = fig.add_subplot(5, 2, 2)
        self.ax22 = fig.add_subplot(5, 2, 4)
        self.ax22.axis('off')
        self.ax23 = fig.add_subplot(5, 2, 6)
        self.ax23.axis('off')
        self.ax24 = fig.add_subplot(5, 2, 8)
        self.ax24.axis('off')
        self.ax25 = fig.add_subplot(5, 2, 10)
        self.ax25.axis('off')
        # self.ax26 = fig.add_subplot(6, 2, 10)
        # self.ax26.axis('off')

        plt.autoscale(False)
        for axx in ax:
            axx[0].set_xticks([])
            axx[0].set_yticks([])
            axx[1].set_xticks([])
            axx[1].set_yticks([])

        # plt.gca().ticklabel_format(axis='y', style='plain')
        # self.ylim_min = self.best_bid_price_history[-1] * (1 - self.limdif)
        # self.ylim_max = self.best_ask_price_history[-1] * (1 + self.limdif)
        # self.ax1.set_ylim(self.ylim_min, self.ylim_max)
        plt.subplots_adjust(left=0.05, right=.98, top=.95, bottom=0.05, hspace=0.01, wspace=0.01)
        # plt.tight_layout()
        ani = animation.FuncAnimation(fig, self.animate_plot, interval=1000 * 1)

        fig.canvas.manager.window.wm_geometry(("+0+500"))

        plt.show()


    def print_monitor(self):
        while True:
            if not self.ready_to_plot:
                self.tc.get("/root/transfer.npy", "./monitor_data/transfer.npy")
                self.tc.get("/root/transfer_status.pkl", "./monitor_data/transfer_status.pkl")
                try:
                    trsf = np.load("./monitor_data/transfer.npy")

                    self.best_bid_price_history = trsf[0][self.last:]
                    self.best_ask_price_history = trsf[1][self.last:]
                    self.smoot_price_history = trsf[2][self.last:]
                    self.decision_history = trsf[3][self.last:]
                    self.market_speed_array_avg = trsf[4][self.last:]
                    self.market_speed_array = trsf[5][self.last:]
                    self.bid_ask_spread_avg = trsf[6][self.last:]
                    self.bid_ask_spread = trsf[7][self.last:]
                    self.best_ask_qty_history = trsf[8][self.last:]
                    self.best_bid_qty_history = trsf[9][self.last:]
                    self.smoot_fast_price_history = trsf[10][self.last:]
                    # self.qty_way_history = trsf[10][self.last:]
                    # status = pd.read_pickle("./monitor_data/LOB_STATUS.pkl")
                    # print(status)

                    with open('./monitor_data/transfer_status.pkl', 'rb') as handle:
                        self.transfer_status = pickle.load(handle)
                        self.transfer_status = self.transfer_status['BTCUSDT_SX3']

                    # print(self.transfer_status)

                    self.ready_to_plot = True
                except:
                    pass
            time.sleep(7)

    def animate_plot(self, i):

        # degree = 36
        # polyreg = make_pipeline(PolynomialFeatures(degree), LinearRegression())
        # polyreg.fit(self.xaxis, self.best_ask_price_history)
        # y_pred = polyreg.predict(self.xaxis)

        if self.ready_to_plot and self.last_best_bid_price_history != np.sum(self.best_bid_price_history):
            self.last_best_bid_price_history = np.sum(self.best_bid_price_history)

            self.ax11.clear()

            self.ax11.margins(x=0.01)
            # print(len(self.best_bid_price_history[self.best_bid_price_history > 0]))
            self.ylim_min = np.min(self.best_bid_price_history[self.best_bid_price_history > 0])
            self.ylim_max = np.max(self.best_ask_price_history[self.best_ask_price_history > 0])

            self.ax11.set_ylim([self.ylim_min, self.ylim_max])
            self.ax11.ticklabel_format(axis='y', style='plain', useOffset=False)
            self.ax11.xaxis.set_ticks(np.arange(0, self.time_period, 500))

            # print("avg bid ask spred",np.mean(self.best_ask_price_history - self.best_bid_price_history))
            # self.ax1.set_yticks(np.range(self.ylim_min,self.ylim_max,.5))

            self.ax11.plot(self.xaxis, self.smoot_price_history, 'k-', linewidth=2)
            self.ax11.plot(self.xaxis, self.smoot_fast_price_history, 'k--', linewidth=1)
            # self.ax1.plot(self.xaxis, self.best_smoot2_price_history, linewidth=1)
            # self.ax1.plot(self.xaxis, y_pred, linewidth=1)
            self.ax11.plot(self.xaxis, self.best_ask_price_history, 'b-', alpha=0.6, linewidth=1)
            self.ax11.plot(self.xaxis, self.best_bid_price_history, 'r-', alpha=0.6, linewidth=1, )
            if self.transfer_status['income_price'] != 0:
                self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['income_price_short']), 'b-', alpha=1, linewidth=2, )
                self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['stop_price']), 'r-', alpha=1, linewidth=2, )
                self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
                self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
                self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['take_price']), 'y-', alpha=1, linewidth=2, )

            # self.ax1.plot(self.xaxis, self.best_mid_price_history_ma_fast, '--', linewidth=1)
            # self.ax1.plot(self.xaxis, self.best_mid_price_history_ma_slow, '--', linewidth=1)
            self.ax11.legend(['mid', 'best ask', 'best bid'], loc=2)

            # print(self.ylim_min, self.ylim_max)
            # self.ax1.plot(x, y_bid)
            self.ax12.clear()
            self.ax12.margins(x=0.01)
            self.ax12.get_yaxis().set_ticks([])

            self.market_speed_array[self.market_speed_array > 400] = 400
            self.market_speed_array_avg[self.market_speed_array_avg > 400] = 400
            # avg1 = np.mean(self.market_speed_array)
            self.market_speed_array[self.market_speed_array == 0] = np.min(self.market_speed_array[self.market_speed_array > 0])
            # avg2 = np.mean(self.market_speed_array_avg)
            self.market_speed_array_avg[self.market_speed_array_avg == 0] = np.min(self.market_speed_array_avg[self.market_speed_array_avg > 0])


            self.ax12.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax12.plot(self.xaxis, self.market_speed_array, 'y-', alpha=0.9, linewidth=2)
            self.ax12.plot(self.xaxis, self.market_speed_array_avg, 'r-', alpha=0.6, linewidth=2)
            self.ax12.legend(['speed', 'speed_avg'], loc=2)

            self.ax13.clear()
            self.ax13.margins(x=0.01)
            self.ax13.get_yaxis().set_ticks([])

            self.ax13.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax13.plot(self.xaxis, self.best_ask_qty_history, 'b-', alpha=0.7, linewidth=2)
            self.ax13.plot(self.xaxis, self.best_bid_qty_history, 'r-', alpha=0.7, linewidth=2)
            self.ax13.legend(['ask qty', 'bid qty'], loc=2)

            self.ax14.clear()
            self.ax14.margins(x=0.01)
            self.ax14.get_yaxis().set_ticks([])

            self.ax14.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax14.plot(self.xaxis, self.bid_ask_spread, 'y-', alpha=0.9, linewidth=2)
            self.ax14.plot(self.xaxis, self.bid_ask_spread_avg, 'g-', alpha=0.8, linewidth=2)
            self.ax14.legend(['b/a spread', 'b/a spread_avg'], loc=2)

            self.ax15.clear()
            self.ax15.margins(x=0.01)
            self.ax15.get_yaxis().set_ticks([])
            self.ax15.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.ax15.plot(self.xaxis, self.decision_history)

            # self.ax16.clear()
            # self.ax16.margins(x=0.01)
            # self.ax16.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.ax16.set_ylim([-5000, +5000])
            # self.ax16.plot(self.xaxis, self.qty_way_history)

            self.ax21.clear()
            self.ax21.set_ylim([self.ylim_min, self.ylim_max])
            self.ax21.get_yaxis().set_ticks([])
            self.ax21.ticklabel_format(axis='y', style='plain', useOffset=False)
            h = self.smoot_price_history
            self.ax21.hist(h,
                           bins='auto',
                           range=None,
                           weights=None,
                           cumulative=False,
                           bottom=None,
                           histtype=u'bar',
                           align=u'mid',
                           orientation=u'horizontal',
                           rwidth=None,
                           log=False,
                           color=None,
                           label=None,
                           stacked=False)

            self.ready_to_plot = False

if __name__ == '__main__':
    n_tob = LOBMonitor()
    n_tob.strat_threads()
    while True:
        time.sleep(50)

