import sys
import numpy as np
from nDot_crypto_db_connector import nDot_db_connector
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt

np.set_printoptions(threshold=5000)


class OrderBookDataset:

    def __init__(self):
        self.n_cdc = nDot_db_connector()
        self.margin = (0.075 / 100) * 2

    def set_margin(self, margin):
        self.margin = margin

    def get_x(self, symbol, dt, x_type=1, qmin=0.985, qmax=1.015, qstep=1000):
        base_dt = dt - timedelta(seconds=1)
        res_tick_base = self.n_cdc.get_tick_data(symbol, base_dt)

        if res_tick_base['last']:
            base_price = float(res_tick_base['last']['price'])
        else:
            base_price = 0

        if base_price > 0:
            # base_price = res_tick_base['last']['price']
            orderbook = self.n_cdc.get_orderbook_data(symbol, dt)
            print("")
            print(base_price)
            print(orderbook['bids'])
            print(orderbook['asks'])

            sys.exit()
            if orderbook:
                if x_type == 1:
                    bids_price_a = []
                    asks_price_a = []
                    bids_vol_a = []
                    asks_vol_a = []
                    for i in range(20):

                        bids_price_a.append(float(orderbook['bids'][i][0]) / base_price)
                        bids_vol_a.append(float(orderbook['bids'][i][1]))

                        asks_price_a.append(float(orderbook['asks'][i][0]) / base_price)
                        asks_vol_a.append(float(orderbook['asks'][i][1]))

                    iret = [bids_price_a,
                            bids_vol_a,
                            asks_price_a,
                            asks_vol_a]

                elif x_type == 2:
                    iret = []
                    for i in range(20):
                        rowa = [float(orderbook['bids'][i][0]) / base_price,
                                float(orderbook['bids'][i][1]),
                                float(orderbook['asks'][i][0]) / base_price,
                                float(orderbook['asks'][i][1])]

                        iret.append(rowa)

                elif x_type == 3:
                    bids_scaled_qt = np.zeros(qstep).astype(np.float32)
                    asks_scaled_qt = np.zeros(qstep).astype(np.float32)
                    # asks_boxs = np.geomspace(qmin, qmax, num=qstep)
                    qr = (qmax - qmin) / qstep
                    asks_boxs = np.arange(qmin, qmax - 1, qr)
                    # print(asks_boxs)
                    # print(orderbook)
                    for i in range(20):
                        # print(float(orderbook['bids'][i][0]) / base_price, float(orderbook['asks'][i][0]) / base_price)
                        # print(float(orderbook['bids'][i][0]) / base_price)
                        bid_pos = np.searchsorted(asks_boxs, float(orderbook['bids'][i][0]) / base_price)
                        ask_pos = np.searchsorted(asks_boxs, float(orderbook['asks'][i][0]) / base_price)

                        # print(ask_pos, bid_pos)

                        bids_scaled_qt[bid_pos] += float(orderbook['bids'][i][1])
                        asks_scaled_qt[ask_pos] += float(orderbook['asks'][i][1])

                    iret = np.concatenate([bids_scaled_qt, asks_scaled_qt])
                    print(iret)

                else:
                    iret = []
            else:
                iret = []
        else:
            iret = []

        return iret

    def get_y(self, symbol, dt, enter_delay_sec, time_window_sec):

        # tick_low_price = []
        tick_high_price = []
        # tick_avg_price = []

        dt_enter = dt + timedelta(seconds=enter_delay_sec)
        res_tick_enter = self.n_cdc.get_tick_data(symbol, dt_enter)
        if len(res_tick_enter['all']) > 0:
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


if __name__ == '__main__':
    symbol = "BTCUSDT"

    n_obds = OrderBookDataset()
    n_obds.set_margin(0.020 / 100)

    date_time_string_ob = "2022-08-01 00:00:00"
    dt = datetime.fromisoformat(date_time_string_ob)
    maxit = 60 * 60 * 24 * 15
    count_all = 0
    count_1 = 0

    x_ds = []
    y_ds = []
    for ix in range(maxit):
        if ix % 5000 == 0:
            print("\r" + f"job ready: {ix} / {maxit} - {count_all} / {count_1} {round((count_1 / (count_all+.001)) *100, 2)}", end="")

        y_sig, y_profit = n_obds.get_y(symbol, dt, enter_delay_sec=2, time_window_sec=10)
        x = n_obds.get_x(symbol, dt, x_type=3, qmin=.9998, qmax=1.0008, qstep=1000)
        # print(x)
        # sys.exit()
        if y_sig in [0, 1] and len(x) > 0 and not np.any(np.isnan(x)) and not np.any(np.isinf(x)):
            x_ds.append(x)
            y_ds.append([y_sig])

            count_all += 1

            if y_sig == 1:
                count_1 += 1

        dt = dt + timedelta(seconds=1)

    print("")
    print(np.array(x_ds).shape)
    print(np.array(y_ds).shape)

    projekt_name = "nDot_DATASET_BTCUSDT_OB"

    gdrive_path = "X:/Apa/cloud/GoogleDriveSync/nDot_Colabs/"

    np.save(gdrive_path + projekt_name + "_X", x_ds)
    np.save(gdrive_path + projekt_name + "_y", y_ds)
