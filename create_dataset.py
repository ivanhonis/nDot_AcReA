import sys
import time
import datetime
import pickle
from datetime import datetime, timedelta

class read_data():

    def __init__(self):
        self.orderbook_data = {}
        self.orderbook_actual_file = ""
        self.tickdata_data = {}
        self.tickdata_actual_file = {}
        self.path_orderbook = "D:/Apa/Coder/crypto_db_ndot/order_book/"
        self.path_tickdata = "D:/Apa/Coder/crypto_db_ndot/tick_data/"

        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT"]

    def get_db_name_orderbook(self, symbol, day_str):
        db_name = symbol + "_" + day_str + ".pickle"
        return self.path_orderbook + symbol + "/" + db_name

    def get_db_name_tickdata(self, symbol, day_str):
        db_name = symbol + "_" + day_str + ".pickle"
        return self.path_tickdata + symbol + "/" + db_name

    def load_orderbook_db(self, symbol, day_str):
        # print('itt2')
        work_file = self.get_db_name_orderbook(symbol, day_str)
        # print(work_file)
        if self.orderbook_actual_file == work_file:
            # print('pass')
            pass
        else:
            # print("try")
            try:
                objectrep = open(work_file, "rb")
                self.orderbook_data = pickle.load(objectrep)
                for ix in self.orderbook_data:
                    print(ix, self.orderbook_data[ix])
                    time.sleep(.1)
                # print(self.orderbook_data)
                self.orderbook_actual_file = work_file
            except:
                self.orderbook_data = {}
                self.orderbook_actual_file = ""

    def get_orderbook_data(self, symbol, dt):

        day_str = self.get_day_str_from_dt_orderbook(dt)
        sec_int = int(self.get_time_no_in_sec_from_dt(dt))

        if symbol in self.symbols:
            # print('itt')
            self.load_orderbook_db(symbol, day_str)
            if sec_int in self.orderbook_data:
                return self.orderbook_data[sec_int]
            else:
                return {}
        else:
            return {}

    def load_tickdata_db(self, symbol, day_str):
        work_file = self.get_db_name_tickdata(symbol, day_str)
        # print(work_file)
        if self.tickdata_actual_file == work_file:
            # print("pass")
            pass
        else:
            # print("betölt")
            try:
                objectrep = open(work_file, "rb")
                self.tickdata_data = pickle.load(objectrep)
                self.tickdata_actual_file = work_file
                # print(self.tickdata_data.keys())
                # print("itt")
                # for i_dd in self.tickdata_data:
                #     print(i_dd, self.tickdata_data[i_dd].keys())
                #     print(i_dd, self.tickdata_data[i_dd]['avg_price'])
                #     print(i_dd, self.tickdata_data[i_dd]['avg_qty'])
                #     print(i_dd, self.tickdata_data[i_dd]['total_qty'])
                #     print(i_dd, self.tickdata_data[i_dd]['tades_count'])
                #     print(i_dd, str(self.tickdata_data[i_dd]['all'])[:100])
                #     print(i_dd, len(self.tickdata_data[i_dd]['all']))
                #     print(i_dd, self.tickdata_data[i_dd]['turnover'])
                #     # print(i_dd, len(self.tickdata_data[i_dd].keys()))

            except:
                self.tickdata_data = {}
                self.tickdata_actual_file = ""

    def get_day_str_from_dt(self, dt):
        return str(dt.year).zfill(4) + str(dt.month).zfill(2) + str(dt.day).zfill(2)

    def get_day_str_from_dt_orderbook(self, dt):
        return str(dt.year).zfill(4) + '_' + str(dt.month).zfill(2) + '_' + str(dt.day).zfill(2)


    def get_time_no_in_sec_from_dt(self, dt):
        return (int(dt.hour) * 60 * 60) + (int(dt.minute) * 60) + int(dt.second)

    def get_tick_data(self, symbol, dt):
        day_str = self.get_day_str_from_dt(dt)
        sec_str = str(self.get_time_no_in_sec_from_dt(dt))
        # print(day_str, sec_str)
        if symbol in self.symbols:
            self.load_tickdata_db(symbol, day_str)
            # for ix in self.tickdata_data:
            #     print(self.tickdata_data[ix])
            #     time.sleep(0.5)
            if sec_str in self.tickdata_data:
                return self.tickdata_data[sec_str].copy()
            else:
                return {}
        else:
            return {}

    def unix_to_datetime(self, ts):
        ts = int(ts)
        return datetime.fromtimestamp(int(ts) / 1000)

if __name__ == '__main__':
    print("hello2")

    n_db = read_data()

    # Data structure a tick datához
    # 'last'
    # 'first'
    # 'avg_price'
    # 'low_price'
    # 'high_price'
    # 'avg_qty'
    # 'total_qty'
    # 'min_qty'
    # 'max_qty'
    # 'trades_count'
    # 'turnover'

    # date_time_string = "2022-06-16 10:10:10"
    # dt = datetime.fromisoformat(date_time_string)
    # print(n_db.get_tick_data("TRXUSDT", dt))
    # print(date_time_string, n_db.unix_to_datetime(n_db.get_tick_data("TRXUSDT", dt)['all'][0]['time']))
    #
    # date_time_string = "2022-06-16 10:10:11"
    # dt = datetime.fromisoformat(date_time_string)
    # print(n_db.get_tick_data("TRXUSDT", dt))
    # print(date_time_string, n_db.unix_to_datetime(n_db.get_tick_data("TRXUSDT", dt)['all'][0]['time']))
    #
    #
    # date_time_string = "2022-06-19 00:00:00"
    # dt = datetime.fromisoformat(date_time_string)
    # total_trades_count = 0
    # for i in range(68400):
    #     res = n_db.get_tick_data("TRXUSDT", dt)
    #     print(i, res['low_price'], res['avg_price'], res['high_price'], res['trades_count'])
    #     total_trades_count += int(res['trades_count'])
    #     # time.sleep(0.5)
    #     dt = dt + timedelta(seconds=1)
    #
    # print('total_trades_count: ', total_trades_count)



    # date_time_string_ob = "2022-05-15 10:10:10"
    # dt1 = datetime.fromisoformat(date_time_string_ob)
    #
    # date_time_string_tick = "2022-05-15 10:10:10"
    # dt2 = datetime.fromisoformat(date_time_string_tick)

    # for ix in range(15):
    #     res_orderbook = n_db.get_orderbook_data("TRXUSDT", dt1)
    #     res_tickdata = n_db.get_tick_data("TRXUSDT", dt2)
    #
    #     dif1 = float(res_tickdata['avg_price']) - float(res_orderbook['data']['bids'][0][0])
    #     dif2 = float(res_orderbook['data']['asks'][0][0]) - float(res_tickdata['avg_price'])
    #
    #     print("low", res_tickdata['low_price'], "hi", res_tickdata['high_price'], "avg", res_tickdata['avg_price'],
    #           "bbid", res_orderbook['data']['bids'][0][0], "bask", res_orderbook['data']['asks'][0][0], dif1, dif2)
    #
    #     dt1 = dt1 + timedelta(seconds=1)
    #     dt2 = dt2 + timedelta(seconds=1)

    date_time_string_ob = "2022-05-15 00:00:00"
    dt1 = datetime.fromisoformat(date_time_string_ob)

    for ix in range(86400):
        res_orderbook = n_db.get_orderbook_data("TRXUSDT", dt1)
        if not res_orderbook:
            print(ix, "gap", dt1, res_orderbook)
        # print(res_orderbook)
        dt1 = dt1 + timedelta(seconds=1)