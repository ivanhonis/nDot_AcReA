import os
import sys
from os import listdir
from os.path import isfile, join
import pickle
import datetime


class DataReorganizer:
    orderbook_unpacked = []
    filelist_orderbook = []
    w_data = {}
    w_actual_file = {}
    r_data = {}
    r_actual_file = {}

    def __init__(self):
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "ATOMUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT"]

        self.path_orderbook = "D:/Apa/Coder/Binance_orderbook_history/2_series_2022_5_02/"
        self.path_orderbook_reorg = "D:/Apa/Coder/crypto_db_ndot/order_book/"
        self.orderbook_unpacked = self.load_orderbook_unpacked()
        self.filelist_orderbook = self.get_filelist_orderbook()
        self.w_data = self.get_defa_dict("dict")
        self.w_actual_file = self.get_defa_dict("")
        self.r_data = self.get_defa_dict("dict")
        self.r_actual_file = self.get_defa_dict("")
        print(self.w_data, self.w_actual_file)

    def get_path_db_name(self, symbol, day_str):
        db_name = symbol + "_" + day_str + ".pickle"
        return self.path_orderbook_reorg + symbol + "/" + db_name

    def w_load_db(self, symbol, day_str):
        work_file = self.get_path_db_name(symbol, day_str)
        if self.w_actual_file[symbol] == work_file:
            pass
        else:
            self.w_save_db(symbol)
            try:
                objectrep = open(work_file, "rb")
                self.w_data[symbol] = pickle.load(objectrep)
                self.w_actual_file[symbol] = work_file
            except:
                self.w_data[symbol] = {}
                self.w_actual_file[symbol] = work_file

    def w_save_db(self, symbol):
        if self.w_actual_file[symbol] != "":
            pickle.dump(self.w_data[symbol], open(self.w_actual_file[symbol], "wb"))

    def w_close_db(self):
        for sy in self.symbols:
            self.w_save_db(sy)

    def add_data(self, symbol, day_str, sec_no, data):
        self.w_load_db(symbol, day_str)
        self.w_data[symbol][sec_no] = data

    def r_load_db(self, symbol, day_str):
        work_file = self.get_path_db_name(symbol, day_str)
        if self.r_actual_file[symbol] == work_file:
            pass
        else:
            try:
                objectrep = open(work_file, "rb")
                self.r_data[symbol] = pickle.load(objectrep)
                self.r_actual_file[symbol] = work_file
            except:
                self.r_data[symbol] = {}
                self.r_actual_file[symbol] = ""

    def get_data(self, symbol, day_str, sec_no):
        if symbol in self.symbols:
            self.r_load_db(symbol, day_str)
            if sec_no in self.r_data[symbol]:
                return self.r_data[symbol][sec_no]
            else:
                return {}
        else:
            return {}

    def get_defa_dict(self, dict_or_str):
        ret_dic = {}
        for sy in self.symbols:
            if dict_or_str == "dict":
                ret_dic[sy] = {}
            else:
                ret_dic[sy] = ""
        return ret_dic

    def get_filelist_orderbook(self):
        files = []
        for i in range(12):
            mypath = self.path_orderbook
            mypath += str(i)
            if os.path.exists(mypath):
                # files += [f for f in listdir(mypath) if isfile(join(mypath, f))]
                for f in listdir(mypath):
                    if isfile(join(mypath, f)):
                        files.append(str(i) + "/" + f)

        return files

    def save_orderbook_unpacked(self):
        pickle.dump(self.orderbook_unpacked, open("orderbook_unpacked.pickle", "wb"))

    def load_orderbook_unpacked(self):
        try:
            objectrep = open("orderbook_unpacked.pickle", "rb")
            return pickle.load(objectrep)
        except:
            return []

    def set_orderbook_unpacked(self, file_name):
        if file_name not in self.orderbook_unpacked:
            self.orderbook_unpacked.append(file_name)
            self.save_orderbook_unpacked()

    def get_dict_by_filename(self, file_name):
        try:
            objectrep = open(self.path_orderbook + file_name, "rb")
            return pickle.load(objectrep)
        except:
            return []

    def get_dt_symbol(self, data):
        # print(data)
        symbol = data['stream'].split("@")[0].upper()
        dt = data['datetime']
        day_str = (str(dt.year) + "_" + ("00" + str(dt.month))[-2:] + "_" + ("00" + str(dt.day))[-2:])
        rtime = datetime.time(dt.hour, dt.minute, dt.second)
        sec_no = (rtime.hour * 60 + rtime.minute) * 60 + rtime.second
        return symbol, day_str, rtime, sec_no

    def cut_msec(self, dt):
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


if __name__ == '__main__':
    dr = DataReorganizer()
    print(dr.orderbook_unpacked)
    dr.set_orderbook_unpacked("f2")
    print(dr.orderbook_unpacked)
    filelist_ob = dr.filelist_orderbook
    print(filelist_ob)
    filelist_ob.sort(key=lambda x: int(''.join(filter(str.isdigit, x))))
    print(filelist_ob)
    # data = [1, 2, 3]
    # dr.add_data("BTCUSDT", "2022_01_01", 2, data)
    # data = [3, 2, 1]
    # dr.add_data("BTCUSDT", "2022_01_01", 2, data)
    # data = [1, 2, 3, 4]
    # dr.add_data("BTCUSDT", "2022_01_01", 3, data)
    #
    # data = [1, 2, 3, 5]
    # dr.add_data("BTCUSDT", "2022_01_02", 2, data)
    # data = [1, 2, 3, 6]
    # dr.add_data("BTCUSDT", "2022_01_02", 3, data)
    # dr.w_close_db()
    #
    # print(dr.get_data("BTCUSDT", "2022_01_01", 2))
    # print(dr.get_data("BTCUSDT", "2022_01_01", 3))
    # print(dr.get_data("BTCUSDT", "2022_01_02", 2))
    # print(dr.get_data("BTCUSDT", "2022_01_02", 3))
    # print(dr.get_data("BTCUSDTc", "2022_01_02", 6))

    symbol_dict = {}
    sy_no = 0
    dict_data = dr.get_dict_by_filename("1/nDotBNC_A_2.pickle")
    for dd in dict_data:
        if dict_data[dd]['stream'] == "btcusdt@depth20":
            odt = dict_data[dd]['datetime']
            c_dt = datetime.datetime(odt.year, odt.month, odt.day, odt.hour, odt.minute, odt.second)
            symbol_dict[sy_no] = dict_data[dd]
            symbol_dict[sy_no]['mdt'] = c_dt
            sy_no += 1

    dkeys = list(symbol_dict.keys())
    dkeys = dkeys[::-1]
    print(dkeys)

    symbol_dict_mod = {}
    for kid in range(len(dkeys)-1):
        act = symbol_dict[dkeys[kid]]
        act_dt = dr.cut_msec(act['datetime'])

        next = symbol_dict[dkeys[kid + 1]]
        next_dt = dr.cut_msec(next['datetime'])

        dif_dt = act_dt-next_dt
        dif_df_int = int(str(dif_dt.seconds))
        if dif_df_int == 1:
            symbol_dict_mod[dkeys[kid]] = act_dt
        else:
            symbol_dict_mod[dkeys[kid]] = act_dt

        print(next_dt, act_dt, dif_df_int)

    sys.exit(0)

    base_dt = symbol_dict[0]['datetime']
    print(base_dt)

    base_dt = datetime.datetime(base_dt.year, base_dt.month, base_dt.day, base_dt.hour,
                                base_dt.minute, base_dt.second)

    print(base_dt)

    for nd in symbol_dict:
        mod_dt = base_dt + datetime.timedelta(seconds=nd)
        print(nd, symbol_dict[nd]['stream'], symbol_dict[nd]['datetime'], mod_dt)




    l_sec_no = 0
    for fo in filelist_ob[0:200]:
        data_dict = dr.get_dict_by_filename(fo)
        # print(data_dict[0])
        for dd in data_dict:
            # print(data_dict[dd])
            symbol, day_str, rtime, sec_no = dr.get_dt_symbol(data_dict[dd])
            if symbol == 'BTCUSDT':
                if sec_no - 1 != l_sec_no:
                    print(symbol, day_str, rtime, sec_no, fo)
                l_sec_no = sec_no
                    # dr.add_data(symbol, day_str, sec_no, data_dict[dd])
                # print(len(str(data_dict[dd])))
        print("-" * 80)
    # dr.w_close_db()

    # for i in range(90000):
    #     d = dr.get_data("BTCUSDT", "2022_05_02", i)
    #     if not d:
    #         print(i)
