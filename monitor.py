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
import pyfastcopy
import shutil
import webbrowser

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
            # {'server_name': "AcReA3",
            #  'slot': 3},
            {'server_name': "AcReA4",
             'slot': 4},
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
            self.p[sid]['fig'], self.p[sid]['ax'] = plt.subplots(5, 1,
                                                                 gridspec_kw={'height_ratios': [2, 4, 1, 1, 2]},
                                                                 figsize=(8.7, 4),
                                                                 num=sid + 1)

            self.p[sid]['fig'].canvas.mpl_connect('close_event', self.on_close)

            self.p[sid]['ax11'] = self.p[sid]['fig'].add_subplot(5, 1, 1)
            self.p[sid]['ax12'] = self.p[sid]['fig'].add_subplot(5, 1, 2)
            self.p[sid]['ax13'] = self.p[sid]['fig'].add_subplot(5, 1, 3)
            self.p[sid]['ax14'] = self.p[sid]['fig'].add_subplot(5, 1, 5)
            self.p[sid]['ax15'] = self.p[sid]['fig'].add_subplot(5, 1, 4)

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

            # if self.d[sid]['status']['simulation'] == 1:
            #     self.p[sid]['ax14'].set_facecolor('#666666')
            # else:
            #     self.p[sid]['ax14'].set_facecolor('#AB6A6E')
            self.p[sid]['ax14'].set_facecolor('#666666')

            # self.ax14.margins(x=0)
            # self.ax14.set_ylim(100, 800)
            self.p[sid]['ax14'].get_yaxis().set_ticks([])
            self.p[sid]['ax14'].xaxis.set_major_formatter(plt.NullFormatter())
            # self.p[sid]['ax14'].spines['bottom'].set_visible(False)
            # self.ax14.xaxis.set_ticks(np.arange(0, self.time_period, 500))
            x = .17
            s1 = .1
            s2 = s1 + x
            s3 = s2 + x
            s4 = s3 + x
            s5 = s4 + x

            self.p[sid]['ax14'].text(0.01, s5, self.d[sid]['riport0'], style='normal', fontsize=8, color="#ffffff")
            self.p[sid]['ax14'].text(0.01, s4, self.d[sid]['riport1'], style='normal', fontsize=8, color="#ffffff")
            self.p[sid]['ax14'].text(0.01, s3, self.d[sid]['riport2'], style='normal', fontsize=8, color="#ffffff")
            self.p[sid]['ax14'].text(0.01, s2, self.d[sid]['riport3'], style='normal', fontsize=8, color="#ffffff")
            self.p[sid]['ax14'].text(0.01, s1, self.d[sid]['riport4'], style='normal', fontsize=8, color="#ffffff")

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

            self.p[sid]['ax11'].legend(['rs slow', 'rs fast', 'ask', 'bid'], loc=2)

            # ZOOM

            self.p[sid]['ax12'].clear()
            self.p[sid]['ax12'].margins(x=0)
            self.p[sid]['ax12'].xaxis.set_major_formatter(plt.NullFormatter())
            # if self.d[sid]['slot_position']['income_price'] != 0:
            #     self.d[sid]['ylim_min'] = np.min(self.d[sid]['best_bid_price_history'][self.zoom_part:][self.d[sid]['best_bid_price_history'][self.zoom_part:] > 0]) - .5
            #     self.d[sid]['ylim_max'] = np.max(self.d[sid]['best_ask_price_history'][self.zoom_part:][self.d[sid]['best_ask_price_history'][self.zoom_part:] > 0]) + .5
            #
            #     self.d[sid]['ylim_min'] = np.min([self.d[sid]['ylim_min'], self.d[sid]['slot_position']['stop_price']]) - .5
            #     self.d[sid]['ylim_max'] = np.max([self.d[sid]['ylim_max'], self.d[sid]['slot_position']['take_price']]) + .5
            # else:
            #     self.d[sid]['ylim_min'] = np.min(self.d[sid]['best_bid_price_history'][self.zoom_part:][self.d[sid]['best_bid_price_history'][self.zoom_part:] > 0]) - .5
            #     self.d[sid]['ylim_max'] = np.max(self.d[sid]['best_ask_price_history'][self.zoom_part:][self.d[sid]['best_ask_price_history'][self.zoom_part:] > 0]) + .5

            self.d[sid]['ylim_min'] = np.min(self.d[sid]['best_bid_price_history'][self.zoom_part:][self.d[sid]['best_bid_price_history'][self.zoom_part:] > 0]) - .5
            self.d[sid]['ylim_max'] = np.max(self.d[sid]['best_ask_price_history'][self.zoom_part:][self.d[sid]['best_ask_price_history'][self.zoom_part:] > 0]) + .5

            self.p[sid]['ax12'].set_ylim([self.d[sid]['ylim_min'], self.d[sid]['ylim_max']])
            self.p[sid]['ax12'].ticklabel_format(axis='y', style='sci', useOffset=False)
            self.p[sid]['ax12'].xaxis.set_ticks(np.arange(0, abs(self.zoom_part), 500))

            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['uniform_bid_price_history'][self.zoom_part:], 'k-', linewidth=2)
            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['uniform_ask_price_history'][self.zoom_part:], 'k--', linewidth=1)

            # self.ax12.plot(self.xaxis_zoom, self.renko_slow_price_history[self.zoom_part:], 'm-', linewidth=2)
            # self.ax12.plot(self.xaxis_zoom, self.renko_fast_price_history[self.zoom_part:], 'm--', linewidth=1)

            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['best_ask_price_history'][self.zoom_part:], 'b-', alpha=0.6, linewidth=1)
            self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['best_bid_price_history'][self.zoom_part:], 'r-', alpha=0.6, linewidth=1, )
            # if self.d[sid]['slot_position']['income_price'] != 0:
            #     self.p[sid]['ax12'].plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.d[sid]['slot_position']['income_price']), 'b-', alpha=1, linewidth=2, )
            #     self.p[sid]['ax12'].plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.d[sid]['slot_position']['stop_price']), 'r-', alpha=1, linewidth=2, )
            #     self.p[sid]['ax12'].plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.d[sid]['slot_position']['trailer_stop_price']), 'y--', alpha=1, linewidth=2, )
            #     self.p[sid]['ax12'].plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.d[sid]['slot_position']['trailer_minimum_price']), 'c--', alpha=1, linewidth=2, )
            #     self.p[sid]['ax12'].plot(self.xaxis_zoom, np.full(abs(self.zoom_part), self.d[sid]['slot_position']['take_price']), 'y-', alpha=1, linewidth=2, )

            self.p[sid]['ax12'].legend(['sm slow', 'sm fast', 'rs slow', 'rf fast', 'ask', 'bid'], loc=2)

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

            self.p[sid]['ax13'].clear()
            self.p[sid]['ax13'].margins(x=0)
            self.p[sid]['ax13'].xaxis.set_major_formatter(plt.NullFormatter())
            self.p[sid]['ax13'].set_ylim(-600, 600)
            self.p[sid]['ax13'].get_yaxis().set_ticks([])
            self.p[sid]['ax13'].xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.p[sid]['ax13'].set_facecolor('#efefef')

            # self.d[sid]['decision_history'][self.d[sid]['decision_history'] == 0] = np.nan
            self.p[sid]['ax13'].plot(self.xaxis_zoom, self.d[sid]['decision_history'][self.zoom_part:], 'g.', linewidth=2)
            # self.p[sid]['ax13'].bar(self.xaxis_zoom_1s, self.d[sid]['decision_history'][self.zoom_part:], width=0.8)

            self.p[sid]['ax15'].clear()
            self.p[sid]['ax15'].margins(x=0)
            self.p[sid]['ax15'].xaxis.set_major_formatter(plt.NullFormatter())
            # self.p[sid]['ax15'].set_ylim(900, 1100)
            self.p[sid]['ax15'].get_yaxis().set_ticks([])
            self.p[sid]['ax15'].xaxis.set_ticks(np.arange(0, self.time_period, 500))
            self.p[sid]['ax15'].set_facecolor('#efefef')


            # self.d[sid]['decision_history'][self.d[sid]['decision_history'] == 0] = np.nan
            self.p[sid]['ax15'].plot(self.xaxis_zoom, self.d[sid]['value_history'][self.zoom_part:], 'y-', linewidth=1)
            # self.p[sid]['ax13'].bar(self.xaxis_zoom_1s, self.d[sid]['decision_history'][self.zoom_part:], width=0.8)

            # except:
            #     pass
            #     # print("plot error.")

if __name__ == '__main__':

    n_tob = LOBMonitor()

    while True:
        time.sleep(50)

