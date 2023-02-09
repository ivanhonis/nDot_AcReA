import os
import sys
import time
import pickle
import numpy as np
# import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import seaborn as sns
# from scipy.signal import savgol_filterz
# import asyncio
from threading import Thread
import psutil
import random
# import pyfastcopy
import shutil
# import webbrowser

np.set_printoptions(threshold=5000)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 2000)
# pd.set_option('display.float_format', '{:8,.4f}'.format)
# pd.set_option('display.max_colwidth', None)
# from server_connection import server_connection


class LOBMonitor:

    def __init__(self):
        self.servers = [
            # {'server_name': "AcReA1",
            #  'slot': 1},
            # {'server_name': "AcReA2",
            #  'slot': 2},
            {'server_name': "AcReA3",
             'slot': 3},
            # {'server_name': "AcReA4",
            #  'slot': 4},
            {'server_name': "AcReA5",
             'slot': 5},
        ]
        self.servers_count = len(self.servers)

        self.zoom_part = -6000
        self.time_period = 20000

        # self.transfer_position = {}
        # self.transfer_statistics = {}

        # plot
        self.xaxis = np.arange(0, self.time_period)
        self.xaxis = self.xaxis.reshape((-1, 1))
        self.xaxis_zoom = np.arange(0, abs(self.zoom_part))
        self.xaxis_zoom = self.xaxis_zoom.reshape((-1, 1))
        self.xaxis_zoom_1s = np.arange(0, abs(self.zoom_part))

        self.p = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]  # plt
        self.d = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]  # data
        self.tc = [[], [], [], [], [], [], [], [], [], [], []]  # connection

        # defaults
        # self.start_webbrowser_thread()

        for i in range(self.servers_count):
            sid = self.servers[i]['slot']
            self.d[sid]['last_best_bid_price_history'] = 0.0
            self.d[sid]['server_name'] = self.servers[i]['server_name']
            # self.d[i]['ready_to_plot'] = False
            # self.tc[i] = server_connection()

        for i in range(self.servers_count):
            self.start_data_read_thread(server_id=self.servers[i]['slot'])

        time.sleep(3)
        self.start_create_chart_thread()

    # def start_async_data_transfer(self, connect, server_id):
    #     asyncio.run(self.data_transfer(connect, server_id))

    # def start_async_data_transfer_multy(self):
    #     asyncio.run(self.data_transfer_multy())

    @staticmethod
    def on_close(event):
        print('exit')
        current_system_pid = os.getpid()
        process = psutil.Process(current_system_pid)
        process.terminate()

    # def start_multy_server_threads(self):
    #     self.task = Thread(target=self.start_async_data_transfer_multy, args=[])
    #     self.task.start()

    def start_webbrowser_thread(self):
        task = Thread(target=self.start_webbrowser, args=[])
        task.start()

    def start_webbrowser(self):
        url = 'https://www.binance.com/en/trade/BTC_BUSD?_from=markets&theme=dark&type=spot'
        webbrowser.open(url, new=200)

    def start_data_read_thread(self, server_id):
        task = Thread(target=self.data_read, args=[server_id])
        task.start()

    def start_create_chart_thread(self):
        sns.set_theme(style="whitegrid", font_scale=.6)
        ani = [[], [], [], [], [], [], [], [], [], [], [], []]
        time.sleep(3)

        wm_geometry = ['+0+0',
                       '+1920+0',
                       '+0+1030',
                       '+1920+1030']

        for i in range(len(self.servers)):
            sid = self.servers[i]['slot']
            self.p[sid]['fig'], self.p[sid]['ax'] = plt.subplots(4, 1,
                                                                 gridspec_kw={'height_ratios': [1.5, 4, 1.5, 2.2]},
                                                                 figsize=(8.65, 4),
                                                                 num=sid + 1)

            self.p[sid]['fig'].canvas.mpl_connect('close_event', self.on_close)

            self.p[sid]['ax11'] = self.p[sid]['fig'].add_subplot(4, 1, 1)
            self.p[sid]['ax12'] = self.p[sid]['fig'].add_subplot(4, 1, 2)
            # self.p[sid]['ax13'] = self.p[sid]['fig'].add_subplot(5, 1, 3)
            self.p[sid]['ax14'] = self.p[sid]['fig'].add_subplot(4, 1, 4)
            self.p[sid]['ax15'] = self.p[sid]['fig'].add_subplot(4, 1, 3)

            plt.autoscale(False)
            for axx in self.p[sid]['ax']:
                axx.set_xticks([])
                axx.set_yticks([])
                axx.get_yaxis().set_visible(False)
                axx.xaxis.set_major_formatter(plt.NullFormatter())
                axx.spines['bottom'].set_visible(False)

            plt.subplots_adjust(left=0.06, right=1, top=1, bottom=0, hspace=-0.01, wspace=0.01)
            ani[sid] = animation.FuncAnimation(self.p[sid]['fig'], self.animate_plot, interval=1000 * 2, fargs=(sid,))
            self.p[sid]['fig'].canvas.manager.window.wm_geometry(wm_geometry[i])
        plt.show()

    # async def download_transfer_timeseries(self, connect, server_id):
    #     connect.get("/root/transfer_timeseries.npz", "./monitor_data/transfer_timeseries_" + str(server_id) + ".npz")
    #
    # async def download_transfer_sh_slot_position_array(self, connect, server_id):
    #     connect.get("/root/transfer_sh_slot_position_array.npz", "./monitor_data/transfer_sh_slot_position_array_" + str(server_id) + ".npz")
    #
    # async def download_transfer_sh_trade_time(self, connect, server_id):
    #     connect.get("/root/transfer_sh_trade_time.npz", "./monitor_data/transfer_sh_trade_time_" + str(server_id) + ".npz")
    #
    # async def download_transfer_sh_status_array(self, connect, server_id):
    #     connect.get("/root/transfer_sh_status_array.npz", "./monitor_data/transfer_sh_status_array_" + str(server_id) + ".npz")
    #
    # async def download_transfer_sh_binance_action_limit(self, connect, server_id):
    #     connect.get("/root/transfer_sh_binance_action_limit.npz", "./monitor_data/transfer_sh_binance_action_limit_" + str(server_id) + ".npz")

    def data_read(self, sid):
        path = "./sync_remote/acrea" + str(sid) + "/transfer_all.npz"
        path_temp = "./sync_remote/acrea" + str(sid) + "/transfer_all_temp.npz"
        while True:
            # if sid == 1:
            #     print(os.stat(path))

            # if file_dt != str(os.path.getmtime(path)):
            while True:
                try:
                    shutil.copyfile(path, path_temp)
                    transfer_all = np.load(path_temp)
                    transfer = np.array(transfer_all["array1"])
                    riport = np.array(transfer_all["array2"])
                except:
                    # print('except file copy read')
                    time.sleep(.5)
                    pass
                else:
                    # file_dt = str(os.path.getmtime(path))
                    break

            self.d[sid]['best_bid_price_history'] = transfer[0][:]
            self.d[sid]['best_ask_price_history'] = transfer[1][:]
            self.d[sid]['uniform_bid_price_history'] = transfer[2][:]
            self.d[sid]['uniform_ask_price_history'] = transfer[3][:]
            self.d[sid]['decision_history'] = transfer[4][:]
            self.d[sid]['value_history'] = transfer[5][:]

            self.d[sid]['riport0'] = riport[0][:].decode("utf-8")
            self.d[sid]['riport1'] = riport[1][:].decode("utf-8")
            self.d[sid]['riport2'] = riport[2][:].decode("utf-8")
            self.d[sid]['riport3'] = riport[3][:].decode("utf-8")
            self.d[sid]['riport4'] = riport[4][:].decode("utf-8")
            self.d[sid]['riport5'] = riport[5][:].decode("utf-8")
            self.d[sid]['riport6'] = riport[6][:].decode("utf-8")
            self.d[sid]['riport7'] = riport[7][:].decode("utf-8")
            self.d[sid]['riport8'] = riport[8][:].decode("utf-8")
            self.d[sid]['riport9'] = riport[9][:].decode("utf-8")

            time.sleep(2)

    def animate_plot(self, i, sid):
        if 'last_best_bid_price_history' in self.d[sid].keys() \
                and 'best_bid_price_history' in self.d[sid].keys() \
                and self.d[sid]['last_best_bid_price_history'] != np.sum(self.d[sid]['best_bid_price_history']):
            self.d[sid]['last_best_bid_price_history'] = np.sum(self.d[sid]['best_bid_price_history'])

            # try:
            self.p[sid]['fig'].canvas.manager.set_window_title(self.d[sid]['server_name'])

            # TEXT
            self.p[sid]['ax14'].clear()
            self.p[sid]['ax14'].grid(color='#666666', linestyle='', linewidth=0)

            if self.d[sid]['riport0'][0:10] == "Simulation":
                self.p[sid]['ax14'].set_facecolor('#666666')
            else:
                self.p[sid]['ax14'].set_facecolor('#AB6A6E')

            self.p[sid]['ax14'].get_yaxis().set_ticks([])
            self.p[sid]['ax14'].xaxis.set_major_formatter(plt.NullFormatter())

            lines = 7
            for i in range(lines):
                self.p[sid]['ax14'].text(0.005, 0.14 * i + 0.03, self.d[sid]['riport'+str(lines - i - 1)], style='normal', fontsize=7.85, color="#ffffff")


            # chart
            self.p[sid]['ax11'].clear()
            self.p[sid]['ax11'].margins(x=0)
            self.p[sid]['ax11'].xaxis.set_major_formatter(plt.NullFormatter())
            self.d[sid]['ylim_min'] = np.min(self.d[sid]['best_bid_price_history'][self.d[sid]['best_bid_price_history'] > 0]) - .5
            self.d[sid]['ylim_max'] = np.max(self.d[sid]['best_ask_price_history'][self.d[sid]['best_ask_price_history'] > 0]) + .5

            self.p[sid]['ax11'].set_ylim([self.d[sid]['ylim_min'], self.d[sid]['ylim_max']])
            self.p[sid]['ax11'].ticklabel_format(axis='y', style='sci', useOffset=False)
            self.p[sid]['ax11'].xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.p[sid]['ax11'].set_facecolor('#efefef')

            # self.p[sid]['ax11'].plot(self.xaxis, self.d[sid]['smoot_slow_price_history'], 'k-', linewidth=2)
            # self.p[sid]['ax11'].plot(self.xaxis, self.d[sid]['smoot_fast_price_history'], 'k--', linewidth=1)
            self.p[sid]['ax11'].plot(self.xaxis, self.d[sid]['best_ask_price_history'], 'b-', alpha=0.6, linewidth=1)
            self.p[sid]['ax11'].plot(self.xaxis, self.d[sid]['best_bid_price_history'], 'r-', alpha=0.6, linewidth=1, )
            # if self.transfer_status['income_price'] != 0:
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['income_price_short']), 'b-', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['stop_price']), 'r-', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
            #     self.ax11.plot(self.xaxis, np.full(self.time_period, self.transfer_status['take_price']), 'y-', alpha=1, linewidth=2, )

            self.p[sid]['ax11'].legend(['ask', 'bid'], loc=2)

            # ZOOM

            self.p[sid]['ax12'].clear()
            self.p[sid]['ax12'].margins(x=0)
            self.p[sid]['ax12'].xaxis.set_major_formatter(plt.NullFormatter())
            self.d[sid]['ylim_min'] = np.min(self.d[sid]['best_bid_price_history'][self.zoom_part:][self.d[sid]['best_bid_price_history'][self.zoom_part:] > 0]) - .5
            self.d[sid]['ylim_max'] = np.max(self.d[sid]['best_ask_price_history'][self.zoom_part:][self.d[sid]['best_ask_price_history'][self.zoom_part:] > 0]) + .5

            self.p[sid]['ax12'].set_ylim([self.d[sid]['ylim_min'], self.d[sid]['ylim_max']])
            self.p[sid]['ax12'].ticklabel_format(axis='y', style='sci', useOffset=False)
            self.p[sid]['ax12'].xaxis.set_ticks(np.arange(0, abs(self.zoom_part), 500))

            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['uniform_ask_price_history'][self.zoom_part:], 'b--', linewidth=1)
            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['uniform_bid_price_history'][self.zoom_part:], 'r--', linewidth=1)

            # self.ax12.plot(self.xaxis_zoom, self.renko_slow_price_history[self.zoom_part:], 'm-', linewidth=2)
            # self.ax12.plot(self.xaxis_zoom, self.renko_fast_price_history[self.zoom_part:], 'm--', linewidth=1)

            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['best_ask_price_history'][self.zoom_part:], 'b-', alpha=0.2, linewidth=1)
            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['best_bid_price_history'][self.zoom_part:], 'r-', alpha=0.2, linewidth=1, )

            action_buy = np.array([np.nan] * abs(self.time_period))
            ask = self.d[sid]['best_ask_price_history']
            long_mask = np.where(self.d[sid]['decision_history'] == 500)[0]
            action_buy[long_mask] = ask[long_mask]
            self.p[sid]['ax12'].plot(self.xaxis_zoom, action_buy[self.zoom_part:], marker=(3, 0, 0), markersize=10, linestyle='None')

            action_sell = np.array([np.nan] * abs(self.time_period))
            ask = self.d[sid]['best_bid_price_history']
            short_mask = np.where(self.d[sid]['decision_history'] == -500)[0]
            action_sell[short_mask] = ask[short_mask]
            self.p[sid]['ax12'].plot(self.xaxis_zoom, action_sell[self.zoom_part:], marker=(3, 0, 180), markersize=10, linestyle='None')



            self.p[sid]['ax12'].legend(['uni ask', 'uni bid', 'ask', 'bid'], loc=2)

            # self.p[sid]['ax13'].clear()
            # self.p[sid]['ax13'].margins(x=0)
            # self.p[sid]['ax13'].xaxis.set_major_formatter(plt.NullFormatter())
            # self.p[sid]['ax13'].set_ylim(-600, 600)
            # self.p[sid]['ax13'].get_yaxis().set_ticks([])
            # self.p[sid]['ax13'].xaxis.set_ticks(np.arange(0, self.time_period, 500))
            # self.p[sid]['ax13'].set_facecolor('#efefef')
            #
            # # self.d[sid]['decision_history'][self.d[sid]['decision_history'] == 0] = np.nan
            # self.p[sid]['ax13'].plot(self.xaxis_zoom, self.d[sid]['decision_history'][self.zoom_part:], 'g.', linewidth=2)



            # self.p[sid]['ax13'].bar(self.xaxis_zoom_1s, self.d[sid]['decision_history'][self.zoom_part:], width=0.8)

            self.p[sid]['ax15'].clear()
            self.p[sid]['ax15'].margins(x=0)
            self.p[sid]['ax15'].xaxis.set_major_formatter(plt.NullFormatter())
            # self.p[sid]['ax15'].set_ylim(900, 1100)
            self.p[sid]['ax15'].get_yaxis().set_ticks([])
            self.p[sid]['ax15'].xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.p[sid]['ax15'].set_facecolor('#efefef')

            amount = self.d[sid]['riport0'].split("Deposit:", 1)
            amount = int(amount[1][1:6])
            bline = np.array([amount] * abs(self.zoom_part))
            self.p[sid]['ax15'].plot(self.xaxis_zoom, bline, 'r--', linewidth=1, alpha=0.4)

            sec_text = '25 min'
            mid_val = (np.max(self.d[sid]['value_history'][self.zoom_part:]) + np.min(self.d[sid]['value_history'][self.zoom_part:])) / 2
            for st in range(12):
                self.p[sid]['ax15'].text(st * 500 + 150, mid_val, sec_text, style='normal', fontsize=8, color="#adadad")
            self.p[sid]['ax15'].plot(self.xaxis_zoom, self.d[sid]['value_history'][self.zoom_part:], 'y-', linewidth=1)

            # except:
            #     pass
            #     # print("plot error.")

if __name__ == '__main__':

    n_tob = LOBMonitor()

    while True:
        time.sleep(50)

