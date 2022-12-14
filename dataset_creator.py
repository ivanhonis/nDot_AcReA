# import random
# import sys
import os
# import time

import numpy as np
from nDot_crypto_db_connector import nDot_db_connector
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import RandomOverSampler
from imblearn.over_sampling import SMOTE, ADASYN
from imblearn import over_sampling

np.set_printoptions(threshold=5000)


class OrderBookDataset:

    def __init__(self):
        self.n_cdc = nDot_db_connector()
        self.margin = (0.025 / 100)

    def set_margin(self, margin):
        self.margin = margin

    def get_x_3d(self, symbol, dt, x_type=1, qmin=-100, qmax=100, qstep=.5, depth=3):

        base_prices = []
        orderbooks = []
        if x_type == 5:

            # 5 nél nincs base price mert azt az orderbook tetejének közepe adja
            for d in range(depth):

                base_dt_ob = dt - timedelta(seconds=d)
                res_ob = self.n_cdc.get_orderbook_data(symbol, base_dt_ob)
                if res_ob:
                    orderbooks.append(res_ob)
                else:
                    return []


        elif x_type in [6, 33]:
            base_dt_tick = dt - timedelta(seconds=1)
            res_tick_base = self.n_cdc.get_tick_data(symbol, base_dt_tick)
            if res_tick_base and len(res_tick_base['all']) > 0:
                all_prices = []
                for ra in res_tick_base['all']:
                    all_prices.append(float(ra['price']))

                if len(all_prices) >= 3:
                    bp = (all_prices[-1] + all_prices[-2] + all_prices[-3]) / 3
                else:
                    # TODO megcsinálni rendesen hogy 3 legyen!!!
                    bp = sum(all_prices) / len(all_prices)

                base_prices.append(bp)
                for d in range(depth):
                    base_dt_ob = dt - timedelta(seconds=d)
                    res_ob = self.n_cdc.get_orderbook_data(symbol, base_dt_ob)
                    if res_ob:
                        orderbooks.append(res_ob)
                    else:
                        return []
            else:
                return []

        else:
            for d in range(depth):

                base_dt_tick = dt - timedelta(seconds=d + 1)
                res_tick_base = self.n_cdc.get_tick_data(symbol, base_dt_tick)

                if res_tick_base and res_tick_base['last']:
                    base_prices.append(float(res_tick_base['last']['price']))
                else:
                    return []

                base_dt_ob = dt - timedelta(seconds=d)
                res_ob = self.n_cdc.get_orderbook_data(symbol, base_dt_ob)
                if res_ob:
                    orderbooks.append(res_ob)
                else:
                    return []

        if x_type == 1:

            iret = []
            boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[d]).astype(np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret.append(ir)

            return iret

        elif x_type == 2:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[d], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 32:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[d], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / (max(np.sum(bids_scaled_qt), np.sum(asks_scaled_qt)))
                asks_scaled_qt = asks_scaled_qt / (max(np.sum(bids_scaled_qt), np.sum(asks_scaled_qt)))

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 33:

            iret_pre = []
            set_max = float(0.0)
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[0], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe
                set_max = max(np.sum(bids_scaled_qt), np.sum(asks_scaled_qt), set_max)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            for d in range(depth):
                iret_pre[d][0] = iret_pre[d][0] / set_max
                iret_pre[d][1] = iret_pre[d][0] / set_max

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 3:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[d], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / np.sum(bids_scaled_qt)
                asks_scaled_qt = asks_scaled_qt / np.sum(asks_scaled_qt)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 4 or x_type == 6:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[0], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / np.sum(bids_scaled_qt)
                asks_scaled_qt = asks_scaled_qt / np.sum(asks_scaled_qt)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 5:

            # az ordebok közepe a baseprice

            iret_pre = []
            base_price = (float(orderbooks[0]['bids'][0][0]) + float(orderbooks[0]['asks'][0][0])) / 2
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_price, dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / ((np.sum(bids_scaled_qt) + np.sum(asks_scaled_qt)) / 2)
                asks_scaled_qt = asks_scaled_qt / ((np.sum(bids_scaled_qt) + np.sum(asks_scaled_qt)) / 2)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

        elif x_type == 7:

            iret_pre = []
            for d in range(depth):

                bids_prices = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 0]
                bids_qty = np.array(orderbooks[d]['bids'], dtype=np.float32)[:, 1]

                asks_prices = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 0]
                asks_qty = np.array(orderbooks[d]['asks'], dtype=np.float32)[:, 1]

                base_array = np.full(asks_prices.shape, base_prices[0], dtype=np.float32)

                c1 = (np.divide(bids_prices, base_array) - 1) * 100000
                c2 = (np.divide(asks_prices, base_array) - 1) * 100000

                boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
                bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
                asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)

                for i in range(20):
                    bid_pos = np.searchsorted(boxes, c1[i])
                    ask_pos = np.searchsorted(boxes, c2[i])

                    # print(ask_pos, bid_pos)

                    bids_scaled_qt[bid_pos] += float(bids_qty[i])
                    asks_scaled_qt[ask_pos] += float(asks_qty[i])

                # csak az arányát teszem be a datasetbe

                bids_scaled_qt = bids_scaled_qt / np.sum(bids_scaled_qt)
                asks_scaled_qt = asks_scaled_qt / np.sum(bids_scaled_qt)

                ir = [bids_scaled_qt, asks_scaled_qt]
                iret_pre.append(ir)

            iret_b = []
            iret_a = []
            for i in range(len(iret_pre[0][0])):
                deep_dot_b = []
                deep_dot_a = []
                for d in range(depth):
                    deep_dot_b.append(iret_pre[d][0][i])
                    deep_dot_a.append(iret_pre[d][1][i])
                iret_b.append(deep_dot_b)
                iret_a.append(deep_dot_a)

            return np.array([iret_b, iret_a])

    # def get_x(self, symbol, dt, x_type=1, qmin=-100, qmax=100, qstep=.5):
    #     base_dt = dt - timedelta(seconds=1)
    #     res_tick_base = self.n_cdc.get_tick_data(symbol, base_dt)
    #
    #     if res_tick_base and res_tick_base['last']:
    #         base_price = float(res_tick_base['last']['price'])
    #     else:
    #         base_price = 0
    #
    #     if base_price > 0:
    #         # base_price = res_tick_base['last']['price']
    #         orderbook = self.n_cdc.get_orderbook_data(symbol, dt)
    #
    #         if orderbook:
    #
    #             if x_type == 1:
    #                 bids_price_a = []
    #                 asks_price_a = []
    #                 bids_vol_a = []
    #                 asks_vol_a = []
    #                 for i in range(20):
    #
    #                     bids_price_a.append(float(orderbook['bids'][i][0]) / base_price)
    #                     bids_vol_a.append(float(orderbook['bids'][i][1]))
    #
    #                     asks_price_a.append(float(orderbook['asks'][i][0]) / base_price)
    #                     asks_vol_a.append(float(orderbook['asks'][i][1]))
    #
    #                 iret = [bids_price_a,
    #                         bids_vol_a,
    #                         asks_price_a,
    #                         asks_vol_a]
    #
    #             elif x_type == 2:
    #                 iret = []
    #                 for i in range(20):
    #                     rowa = [float(orderbook['bids'][i][0]) / base_price,
    #                             float(orderbook['bids'][i][1]),
    #                             float(orderbook['asks'][i][0]) / base_price,
    #                             float(orderbook['asks'][i][1])]
    #
    #                     iret.append(rowa)
    #
    #             elif x_type == 3:
    #                 bids_prices = np.array(orderbook['bids'], dtype=np.float32)[:, 0]
    #                 bids_qty = np.array(orderbook['bids'], dtype=np.float32)[:, 1]
    #
    #                 asks_prices = np.array(orderbook['asks'], dtype=np.float32)[:, 0]
    #                 asks_qty = np.array(orderbook['asks'], dtype=np.float32)[:, 1]
    #
    #                 base_array = np.full(asks_prices.shape, base_price).astype(np.double)
    #
    #                 c1 = (np.divide(bids_prices, base_array) - 1) * 100000
    #                 c2 = (np.divide(asks_prices, base_array) - 1) * 100000
    #
    #                 boxes = np.arange(qmin, qmax, qstep).astype(np.float32)
    #                 bids_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
    #                 asks_scaled_qt = np.zeros(len(boxes) + 1).astype(np.float32)
    #
    #                 for i in range(20):
    #
    #                     bid_pos = np.searchsorted(boxes, c1[i])
    #                     ask_pos = np.searchsorted(boxes, c2[i])
    #
    #                     # print(ask_pos, bid_pos)
    #
    #                     bids_scaled_qt[bid_pos] += float(bids_qty[i])
    #                     asks_scaled_qt[ask_pos] += float(asks_qty[i])
    #
    #                 iret = np.concatenate([bids_scaled_qt, asks_scaled_qt])
    #
    #             else:
    #                 iret = []
    #         else:
    #             iret = []
    #     else:
    #         iret = []
    #
    #     return iret

    def get_y(self, symbol, dt, y_type, enter_delay_sec, time_window_sec, margin=.025 / 100, exp_prob=.8325):

        self.margin = margin

        if y_type == 1:

            # tick_low_price = []
            tick_high_price = []
            # tick_avg_price = []

            dt_enter = dt + timedelta(seconds=enter_delay_sec)
            res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
            if res_tick_enter and len(res_tick_enter['all']) > 0:
                enter_price = float(res_tick_enter['avg_price'])

                for ti in range(time_window_sec):
                    dt3 = dt_enter + timedelta(seconds=ti + 1)
                    res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
                    if res_tick2 and len(res_tick2['all']) > 0:
                        # tick_low_price.append(float(res_tick2['low_price']))
                        tick_high_price.append(float(res_tick2['high_price']))
                        # tick_avg_price.append(float(res_tick2['avg_price']))

                if len(tick_high_price) > 0 and enter_price > 0:
                    cost = enter_price * self.margin
                    exit_price = max(tick_high_price)
                    profit = (exit_price - cost) / enter_price
                    if profit > 1:
                        # print(enter_price, exit_price - cost, cost, profit)
                        return 1, profit
                    else:
                        return 0, profit
                else:
                    return 9, 0
            else:
                return 9, 0

        elif y_type == 2:

            tick_low_price = []
            tick_high_price = []
            # tick_avg_price = []

            dt_enter = dt + timedelta(seconds=enter_delay_sec)
            res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
            if res_tick_enter and len(res_tick_enter['all']) > 0:
                # print(res_tick_enter['all'])
                enter_price = float(res_tick_enter['all'][0]['price'])

                for ti in range(time_window_sec):
                    dt3 = dt_enter + timedelta(seconds=ti)
                    res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
                    if res_tick2 and len(res_tick2['all']) > 0:
                        tick_low_price.append(float(res_tick2['low_price']))
                        tick_high_price.append(float(res_tick2['high_price']))
                        # tick_avg_price.append(float(res_tick2['avg_price']))

                if len(tick_high_price) > 0 and enter_price > 0:
                    cost = enter_price * self.margin
                    exit_price = max(tick_high_price)
                    profit = (exit_price - cost) / enter_price
                    if profit > 1 and min(tick_low_price) >= enter_price:
                        # print(enter_price, exit_price - cost, cost, profit)
                        return 1, profit
                    else:
                        return 0, profit
                else:
                    return 9, 0
            else:
                return 9, 0

        if y_type == 3:

            # tick_low_price = []
            tick_high_price = []
            # tick_avg_price = []

            dt_enter = dt + timedelta(seconds=enter_delay_sec)
            res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
            if res_tick_enter and len(res_tick_enter['all']) > 0:
                enter_price = float(res_tick_enter['all'][0]['price'])

                for ti in range(time_window_sec):
                    dt3 = dt_enter + timedelta(seconds=ti)
                    res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
                    if res_tick2 and len(res_tick2['all']) > 0:
                        # tick_low_price.append(float(res_tick2['low_price']))
                        tick_high_price.append(float(res_tick2['high_price']))
                        # tick_avg_price.append(float(res_tick2['avg_price']))

                if len(tick_high_price) > 0 and enter_price > 0:
                    cost = enter_price * self.margin
                    exit_price = max(tick_high_price)
                    profit = (exit_price - cost) / enter_price
                    if profit > 1:
                        # print(enter_price, exit_price - cost, cost, profit)
                        return 1, profit
                    else:
                        return 0, profit
                else:
                    return 9, 0
            else:
                return 9, 0

        if y_type == 4:

            tick_price = []

            dt_enter = dt + timedelta(seconds=enter_delay_sec)
            res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
            if res_tick_enter and len(res_tick_enter['all']) > 0:
                enter_price = float(res_tick_enter['all'][0]['price'])

                tw_count = 0
                for ti in range(time_window_sec):
                    dt3 = dt_enter + timedelta(seconds=ti)
                    res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
                    if res_tick2 and len(res_tick2['all']) > 0:
                        tw_count += 1
                        for ap in res_tick2['all']:
                            tick_price.append(float(ap['price']))

                tick_price = np.array(tick_price)

                if tw_count == time_window_sec:
                    under_enter = (tick_price < enter_price).sum()
                    over_enter = (tick_price >= enter_price).sum()
                    prob = over_enter / (under_enter + over_enter)
                    if prob > exp_prob:
                        # print(max(tick_price) / enter_price)
                        return 1, 0
                    else:
                        return 0, 0
                else:
                    return 9, 0
            else:
                return 9, 0

        if y_type == 5:

            tick_price = []

            dt_enter = dt + timedelta(seconds=enter_delay_sec)
            res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
            if res_tick_enter and len(res_tick_enter['all']) > 0:
                enter_price = float(res_tick_enter['all'][0]['price'])

                tw_count = 0
                for ti in range(time_window_sec):
                    dt3 = dt_enter + timedelta(seconds=ti)
                    res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
                    if res_tick2 and len(res_tick2['all']) > 0:
                        tw_count += 1
                        for ap in res_tick2['all']:
                            tick_price.append(float(ap['price']))

                tick_price = np.array(tick_price)

                if tw_count == time_window_sec:
                    cost = enter_price * self.margin
                    exit_price = max(tick_price)
                    profit = (exit_price - cost) / enter_price

                    under_enter = (tick_price < enter_price).sum()
                    over_enter = (tick_price >= enter_price).sum()
                    prob = over_enter / (under_enter + over_enter)

                    if prob > exp_prob and profit > 1:
                        return 1, 0
                    else:
                        return 0, 0
                else:
                    return 9, 0
            else:
                return 9, 0

    def show_prices(self, dt, time_window_sec, title="show prices"):

        future_prices = []

        for ti in range(time_window_sec):
            dt3 = dt + timedelta(seconds=ti)
            res_tick2 = self.n_cdc.get_tick_data(symbol, dt3)
            if res_tick2 and len(res_tick2['all']) > 0:
                for pb in res_tick2['all']:
                    future_prices.append(float(pb['price']))

        future_prices = np.array(future_prices)
        fp = future_prices[0]
        future_prices = np.divide(future_prices, fp)

        fig, ax = plt.subplots()
        # fig.canvas.manager.window.move(50, 50)
        fig.set_size_inches(14, 6)
        ax.plot(future_prices)

        ax.set(xlabel='time (s)', ylabel='price ($)', title=title)
        ax.grid()
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    gdrive_path = "D:/Clouds/GoogleDrive/nDot_Colabs/"

    n_obds = OrderBookDataset()
    n_obds.set_margin(0.020 / 100)

    projekt_name = "LOB"
    symbol = "BTCUSDT"
    # sub_dataset = "y3_5_20_depth_4"
    # sub_dataset = "y3_5_20"
    sub_dataset = "azoo5"
    depth = 3
    broadcast = 201

    # const
    dataset_name = "nDot_DATASET_" + projekt_name
    dty0, dty1, dtx0, dtx1, dts0, dts1 = 0, 0, 0, 0, 0, 0

    for t_set in range(2):
        # t_set += 1

        if t_set == 0:
            print("Create dataset for TRAIN")

            date_time_string_ob = "2022-10-01 00:00:00"
            dt = datetime.fromisoformat(date_time_string_ob)
            maxit01 = 60 * 60 * 24 * 35
            maxit1 = 60 * 60 * 24 * 0
            # BTC ETH 5 20 10.03

            maxit = maxit01 + maxit1
            count_all = 0
            count_1 = 0

            x_ds = np.zeros((maxit, 2, broadcast, depth), dtype=np.float32)
            y_ds = np.zeros((maxit, 1), dtype=np.int32)

            sufix = "_RGB"

        else:
            print("Create dataset for Test")
            date_time_string_ob = "2022-09-25 00:00:00"
            dt = datetime.fromisoformat(date_time_string_ob)
            maxit01 = 60 * 60 * 24 * 5
            maxit1 = 60 * 60 * 24 * 10 * 0
            maxit = maxit01 + maxit1
            count_all = 0
            count_1 = 0

            x_ds = np.zeros((maxit, 2, broadcast, depth), dtype=np.float32)
            y_ds = np.zeros((maxit, 1), dtype=np.int32)

            sufix = "_RGB_TEST"

        d1 = f"{gdrive_path}{symbol}/"
        d2 = f"{gdrive_path}{symbol}/{sub_dataset}/"

        if not os.path.exists(d1):
            print(f"mkdir: {d1}")
            os.makedirs(d1)

        if not os.path.exists(d2):
            print(f"mkdir: {d2}")
            os.makedirs(d2)

        y_need = [0, 1]

        first = True
        for ix in range(maxit):

            if ix > maxit01:
                y_need = [1]

            if ix % 1000 == 0 and ix != 0:
                print("\r" + f"job ready: {ix} / {maxit} - {count_all} / {count_1} {round((count_1 / (count_all+.001)) *100, 2)}"
                             f"    {dty1-dty0} {dtx1-dtx0} {dts1-dts0}", end="")
                # print("")
                # print(np.max(x_ds))
                # print(np.min(x_ds))
            dty0 = datetime.now()
            # y_sig, y_profit = n_obds.get_y(symbol, dt, y_type=1, enter_delay_sec=0, time_window_sec=2)
            # y_sig, y_profit = n_obds.get_y(symbol, dt, y_type=2, enter_delay_sec=0, time_window_sec=2, margin=.020/100)
            # y_sig, y_profit = n_obds.get_y(symbol, dt, y_type=3, enter_delay_sec=0, time_window_sec=3, margin=.020 / 100)
            # 09.15. 5,15 -  11.01 10,0
            y_sig, y_profit = n_obds.get_y(symbol, dt, y_type=3, enter_delay_sec=0, time_window_sec=3, margin=.02 / 100, exp_prob=.85)

            # print(y_sig)
            # print(np.array(y_sig).shape)
            dty1 = datetime.now()
            # x = n_obds.get_x(symbol, dt, x_type=3, qmin=-50, qmax=50, qstep=.5)
            if y_sig in y_need:
                dtx0 = datetime.now()
                x = n_obds.get_x_3d(symbol, dt, x_type=5, qmin=-50, qmax=50, qstep=.5, depth=depth)
                # print(x.shape)
                dtx1 = datetime.now()
            else:
                x = []
            # print(np.array(x).shape)
            # print(np.array(x).shape)
            # print(np.array(x))
            # sys.exit()
            if y_sig in y_need and len(x) > 0 and not np.any(np.isnan(x)) and not np.any(np.isinf(x)):
                dts0 = datetime.now()
                x_ds[count_all] = x
                y_ds[count_all] = [y_sig]
                dts1 = datetime.now()
                count_all += 1

                if y_sig == 1:
                    count_1 += 1
                    # if random.randint(0, 3) == 2:
                    #     n_obds.show_prices(dt, time_window_sec=3)

            # if ix % 100 == 0 and ix != 0:
            #     dt = dt + timedelta(seconds=(60 * 60 * 24 * 7))
            # else:
            #     dt = dt + timedelta(seconds=random.randint(0, 15))

            # dt = dt + timedelta(seconds=random.randint(0, 5))

            dt = dt + timedelta(seconds=1)

        x_ds = x_ds[0:count_all]
        y_ds = y_ds[0:count_all]

        ou_samle = True
        if ou_samle and t_set == 0:
            ori_shape = x_ds.shape
            x_ds = np.reshape(x_ds, (x_ds.shape[0], x_ds.shape[1] * x_ds.shape[2] * x_ds.shape[3]))
            # X_train_np, y_train_np = RandomOverSampler(sampling_strategy='auto').fit_resample(X_train_np, y_train_np)
            x_ds, y_ds = RandomUnderSampler(sampling_strategy='auto').fit_resample(x_ds, y_ds)
            # x_ds, y_ds = SMOTE().fit_resample(X_train_np, y_train_np)
            x_ds = np.reshape(x_ds, (x_ds.shape[0], ori_shape[1], ori_shape[2], ori_shape[3]))

        print("")
        print("   Last dt:", dt)
        print("x_ds.shape:", x_ds.shape)
        print("y_ds.shape:", y_ds.shape)
        print('      y_ds:', np.unique(y_ds, return_counts=True))

        # print(f"{gdrive_path}{symbol}/{sub_dataset}/{projekt_name}{sufix}_X")
        # print(f"{gdrive_path}{symbol}/{sub_dataset}/{projekt_name}{sufix}_y")

        print(f"Save: {gdrive_path}{symbol}/{sub_dataset}/{dataset_name}{sufix}_X")
        print(f"Save: {gdrive_path}{symbol}/{sub_dataset}/{dataset_name}{sufix}_y")

        np.save(f"{gdrive_path}{symbol}/{sub_dataset}/{dataset_name}{sufix}_X", x_ds)
        np.save(f"{gdrive_path}{symbol}/{sub_dataset}/{dataset_name}{sufix}_y", y_ds)

        print("Ready.")
