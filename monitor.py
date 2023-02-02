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
            {'server_name': "AcReA1",
             'slot': 1},
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
            self.p[sid]['fig'], self.p[sid]['ax'] = plt.subplots(4, 1,
                                                                 gridspec_kw={'height_ratios': [2, 4, 1, 2]},
                                                                 figsize=(8.7, 4),
                                                                 num=sid + 1)

            self.p[sid]['fig'].canvas.mpl_connect('close_event', self.on_close)

            self.p[sid]['ax11'] = self.p[sid]['fig'].add_subplot(4, 1, 1)
            self.p[sid]['ax12'] = self.p[sid]['fig'].add_subplot(4, 1, 2)
            self.p[sid]['ax13'] = self.p[sid]['fig'].add_subplot(4, 1, 3)
            self.p[sid]['ax14'] = self.p[sid]['fig'].add_subplot(4, 1, 4)

            plt.autoscale(False)
            for axx in self.p[sid]['ax']:
                axx.set_xticks([])
                axx.set_yticks([])
                axx.get_yaxis().set_visible(False)
                axx.xaxis.set_major_formatter(plt.NullFormatter())
                axx.spines['bottom'].set_visible(False)

            plt.subplots_adjust(left=0.05, right=.98, top=.95, bottom=0.05, hspace=-0.01, wspace=0.01)
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
                    slot_position_array = np.array(transfer_all["array1"])
                    trade_time_array = np.array(transfer_all["array2"])
                    status_array = np.array(transfer_all["array3"])
                    binance_action_limit_array = np.array(transfer_all["array4"])
                    transfer = np.array(transfer_all["array5"])
                    self.d[sid]['settings_name'] = "  ".join(transfer_all["array6"])
                except:
                    # print('except file copy read')
                    time.sleep(.5)
                    pass
                else:
                    # file_dt = str(os.path.getmtime(path))
                    break

            self.d[sid]['decision_history'] = transfer[7][:]
            self.d[sid]['best_bid_price_history'] = transfer[0][:]
            self.d[sid]['best_ask_price_history'] = transfer[1][:]
            self.d[sid]['renko_slow_price_history'] = transfer[2][:]
            self.d[sid]['renko_fast_price_history'] = transfer[3][:]
            self.d[sid]['renko_stop_price_history'] = transfer[4][:]
            self.d[sid]['smoot_slow_price_history'] = transfer[5][:]
            self.d[sid]['smoot_fast_price_history'] = transfer[6][:]

            self.d[sid]['binance_action_limit'] = {
                "blocked_actions": binance_action_limit_array[0],
                "sum_orders24": binance_action_limit_array[1],
                "max_order": binance_action_limit_array[2],
                "max_request": binance_action_limit_array[3],
            }

            if trade_time_array[0] == 0:
                trade_time_array[0] = 1
            self.d[sid]['trade_time'] = {
                "trade_time_min": np.min(trade_time_array[trade_time_array > 0]),
                "trade_time_max": np.max(trade_time_array[trade_time_array > 0]),
                "trade_time_mean": np.mean(trade_time_array[trade_time_array > 0]),
            }

            self.d[sid]['slot_position'] = {
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
                "exit_value": slot_position_array[10],
            }

            self.d[sid]['status'] = {
                "stop": status_array[0],
                "buy_again": status_array[1],
                "take": status_array[2],
                "trailer": status_array[3],
                "time": status_array[4],
                "profit": status_array[5],
                "turnover": status_array[6],
                "max_qty": status_array[7],
                "min_value": status_array[8],
                "yield": status_array[9],
                "simulation": status_array[10],
            }
            time.sleep(2)

    def get_limit_str(self, sid):
        blocked_actions = self.d[sid]['binance_action_limit']["blocked_actions"]
        sum_orders24 = self.d[sid]['binance_action_limit']["sum_orders24"]
        max_order = self.d[sid]['binance_action_limit']["max_order"]
        max_request = self.d[sid]['binance_action_limit']["max_request"]
        r_str = f"Blocked actions: {blocked_actions}     Sum_orders24 (160000 / 24h): {sum_orders24}     max_order (50 / 10 sec): {max_order}    " \
                f"Max request (1200 / 60 sec): {max_request}   "
        return r_str

    def get_status_str(self, sid):
        monitor_stop = int(self.d[sid]['status']['stop'])
        monitor_take = int(self.d[sid]['status']['take'])
        monitor_trailer = int(self.d[sid]['status']['trailer'])
        monitor_buy_again = int(self.d[sid]['status']['buy_again'])
        monitor_time = int(self.d[sid]['status']['time'])
        monitor_turnover = round(float(self.d[sid]['status']['turnover']), 2)
        monitor_max_qty = round(float(self.d[sid]['status']['max_qty']), 6)
        monitor_min_value = round(float(self.d[sid]['status']['min_value']), 2)
        r_str = f"Take: {monitor_take}   Stop: {monitor_stop}   Trailer: {monitor_trailer}  " \
                f"Buy_again: {monitor_buy_again}   Time: {monitor_time}   " \
                f"Turn_over: {monitor_turnover} USD   Max.qty: {monitor_max_qty} BTC   Min.value: {monitor_min_value} USD"
        return r_str

    def get_profit_str(self, sid):
        monitor_yield = round(float(self.d[sid]['status']['yield']) * 100, 2)
        monitor_profit = round(float(self.d[sid]['status']['profit']), 6)
        r_str = f"Profit: {monitor_profit} USD  {monitor_yield} % / year"
        return r_str

    def get_position_str(self, sid):

        qty = round(self.d[sid]['slot_position']['qty'], 8)
        free_invest_quote = round(self.d[sid]['slot_position']['free_invest_quote'], 4)
        actual_value = round(self.d[sid]['slot_position']['actual_value'], 4)
        actual_profile = round(self.d[sid]['slot_position']['actual_profile'], 0)
        last_buy_price = round(self.d[sid]['slot_position']['last_buy_price'], 8)
        exit_value = round(self.d[sid]['slot_position']['exit_value'], 8)

        r_str = f"Qty: {qty} BTC     free_invest: {free_invest_quote} USD     " \
                f"act._val.: {actual_value} USD   last_buy_prc.: {last_buy_price} USD   act._profile: {actual_profile}   " \
                f"exit_val.: {exit_value}"
        return r_str

    def animate_plot(self, i, sid):
        if 'last_best_bid_price_history' in self.d[sid].keys() \
                and 'best_bid_price_history' in self.d[sid].keys() \
                and self.d[sid]['last_best_bid_price_history'] != np.sum(self.d[sid]['best_bid_price_history']):
            self.d[sid]['last_best_bid_price_history'] = np.sum(self.d[sid]['best_bid_price_history'])

            try:
                self.p[sid]['fig'].canvas.manager.set_window_title(self.d[sid]['server_name'])

                # TEXT
                self.p[sid]['ax14'].clear()
                self.p[sid]['ax14'].grid(color='#666666', linestyle='', linewidth=0)

                if self.d[sid]['status']['simulation'] == 1:
                    self.p[sid]['ax14'].set_facecolor('#666666')
                else:
                    self.p[sid]['ax14'].set_facecolor('#AB6A6E')

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

                self.p[sid]['ax14'].text(0.01, s1, self.get_status_str(sid), style='normal', fontsize=8, color="#ffffff")
                self.p[sid]['ax14'].text(0.01, s2, self.get_limit_str(sid), style='normal', fontsize=8, color="#ffffff")
                self.p[sid]['ax14'].text(0.01, s3, self.get_position_str(sid), style='normal', fontsize=8, color="#ffffff")
                self.p[sid]['ax14'].text(0.01, s4, self.get_profit_str(sid), style='normal', fontsize=8, color="#ffffff")
                self.p[sid]['ax14'].text(0.01, s5, self.d[sid]['settings_name'], style='normal', fontsize=8, color="#ffffff")

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

                self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['smoot_slow_price_history'][self.zoom_part:], 'k-', linewidth=2)
                self.p[sid]['ax12'].plot(self.xaxis_zoom, self.d[sid]['smoot_fast_price_history'][self.zoom_part:], 'k--', linewidth=1)

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

            except:
                pass
                # print("plot error.")

if __name__ == '__main__':

    n_tob = LOBMonitor()

    while True:
        time.sleep(50)

