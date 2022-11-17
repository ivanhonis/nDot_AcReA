import os
import sys
import time
from os import listdir
from os.path import isfile, join
import pickle
import datetime
import psutil
import gc


class Refine_tickdata:

    def __init__(self):
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                        "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                        "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                        "THETAUSDT", "OMGUSDT"]

        # self.defa_data_dict = {'all': [],
        #                        'last': {},
        #                        'first': {},
        #                        'avg_price': 0.0,  ## weigheted with amount
        #                        'avg_amount': 0,
        #                        'total_amount': 0,
        #                        'tades_count': 0}

        # self.defa_sec_dict = self.defa_sec_dict_load()

        self.path_pickdata = "X:/Apa/coder/Binance_tick_data/"
        self.path_pickdata_refined = "X:/Apa/coder/crypto_db_ndot/tick_data/"
        self.filelist_orderbook = self.get_filelist_pickdata()  # x hányas alkönyvtártól hányadikig
        self.w_data = self.get_defa_dict("dict").copy()
        # print(self.w_data.keys())
        # print(self.w_data['ATOMUSDT'].keys())
        # print(self.w_data['ATOMUSDT']['0'].keys())
        # sys.exit(0)
        self.w_actual_file = self.get_defa_dict("")
        self.r_actual_file = self.get_defa_dict("")
        self.tick_data_done_files = []
        self.load_done_files()

    def load_done_files(self):
        try:
            objectrep = open("tick_data_done_files.pickle", "rb")
            self.tick_data_done_files = pickle.load(objectrep)
        except:
            self.tick_data_done_files = []

    def save_done_files(self):
        pickle.dump(self.tick_data_done_files, open("tick_data_done_files.pickle", "wb"))

    def add_done_files(self, fn):
        if fn not in self.tick_data_done_files:
            self.tick_data_done_files.append(fn)
            self.save_done_files()


    def defa_sec_dict_load(self):
        i_defa_sec_dict = {}
        for i_i in range(86400):
            i_defa_sec_dict[str(i_i)] = {'all': [],
                                         'last': {},
                                         'first': {},
                                         'avg_price': 0.0,  ## weigheted with amount
                                         'low_price': 0.0,
                                         'high_price': 0.0,
                                         'avg_qty': 0,
                                         'total_qty': 0,
                                         'min_qty': 0.00000000,
                                         'max_qty': 0.00000000,
                                         'trades_count': 0,
                                         'turnover': 0}
        return i_defa_sec_dict

    def get_defa_dict(self, dict_or_str):

        ret_dic = {}
        for sy in self.symbols:
            if dict_or_str == "dict":
                ret_dic[sy] = self.defa_sec_dict_load()
            else:
                ret_dic[sy] = ""
        return ret_dic

    def get_filelist_pickdata(self):
        files = []
        mypath = self.path_pickdata
        if os.path.exists(mypath):
            # files += [f for f in listdir(mypath) if isfile(join(mypath, f))]
            for f in listdir(mypath):
                if isfile(join(mypath, f)):
                    files.append(f)
        files.sort(key=lambda x: int(''.join(filter(str.isdigit, x))))
        # ez azt csinálja, hogy A1 után A2 jön és nem A11
        return files

    def print_free_mem(self):
        print(round(psutil.virtual_memory().free / 1024 / 1024 / 1027, 2), "GB")

    def get_dict_by_filename(self, file_name):
        try:
            objectrep = open(self.path_pickdata + file_name, "rb")
            return pickle.load(objectrep)
        except:
            return []

    def unix_to_datetime(self, ts):
        ts = int(ts)
        return datetime.datetime.fromtimestamp(int(ts) / 1000)

    def dt_samp_str(self, ts):
        ts = int(ts)
        dt = datetime.datetime.fromtimestamp(int(ts) / 1000)
        return str(dt.year).zfill(4) + str(dt.month).zfill(2) + str(dt.day).zfill(2) \
               + str(dt.hour).zfill(2) + str(dt.minute).zfill(2) + str(dt.second).zfill(2)

    def get_time_no_in_sec(self, ts):
        ts = int(ts)
        dt = datetime.datetime.fromtimestamp(int(ts) / 1000)
        return (int(dt.hour) * 60 * 60) + (int(dt.minute) * 60) + int(dt.second)

    def get_day_str(self, ts):
        ts = int(ts)
        dt = datetime.datetime.fromtimestamp(int(ts) / 1000)
        return str(dt.year).zfill(4) + str(dt.month).zfill(2) + str(dt.day).zfill(2)

    def get_data_from_name(self, name):
        name = name.replace(".pickle", "")
        sep_array = name.split("-")
        return sep_array[0], sep_array[1], sep_array[2]

    def get_symbol(self, fn):
        return fn.split('-')[0]

    # load save

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
                # print("work file: ", symbol, self.w_actual_file[symbol] )
            except:
                self.w_data[symbol] = self.defa_sec_dict_load()
                self.w_actual_file[symbol] = work_file
                # print("work file: ", symbol, self.w_actual_file[symbol])

    def w_save_db(self, symbol):

        if self.w_actual_file[symbol] != "":
            # for i_sec in range(68400):
            #     print(i_sec, len(self.w_data[symbol][str(i_sec)]['all']))

            # sys.exit(0)
            for i_sec in range(86400):
                # print("-" * 80)
                # print(str(i_sec))
                # print(self.w_data[symbol][str(i_sec)]['all'])
                if self.w_data[symbol][str(i_sec)]['all']:
                    turnover = 0
                    total_qty = 0
                    trades_count = 0
                    largest_id = -1
                    largest_id_data = {}
                    smallest_id = 99999999999999999
                    smallest_id_data = {}
                    max_qty = -1
                    min_qty = 99999999999999999
                    high_price = -1
                    low_price = 99999999999999999
                    for i_data in self.w_data[symbol][str(i_sec)]['all']:
                        if int(i_data['id']) > largest_id:
                            largest_id = int(i_data['id'])
                            largest_id_data = i_data

                        if int(i_data['id']) < smallest_id:
                            smallest_id = int(i_data['id'])
                            smallest_id_data = i_data

                        if float(i_data['qty']) > max_qty:
                            max_qty = float(i_data['qty'])

                        if float(i_data['qty']) < min_qty:
                            min_qty = float(i_data['qty'])

                        if float(i_data['price']) > high_price:
                            high_price = float(i_data['price'])

                        if float(i_data['price']) < low_price:
                            low_price = float(i_data['price'])

                        turnover += float(i_data['quoteQty'])
                        total_qty += float(i_data['qty'])
                        trades_count += 1

                    # first_id = smallest_id_data['id']
                    # last_id = largest_id_data['id']
                    avg_price = round(turnover / total_qty, 8)
                    avg_qty = round(total_qty / trades_count, 8)
                    total_qty = round(total_qty, 8)
                    turnover = round(turnover, 8)

                    self.w_data[symbol][str(i_sec)]['last'] = largest_id_data
                    self.w_data[symbol][str(i_sec)]['first'] = smallest_id_data
                    self.w_data[symbol][str(i_sec)]['avg_price'] = avg_price
                    self.w_data[symbol][str(i_sec)]['low_price'] = low_price
                    self.w_data[symbol][str(i_sec)]['high_price'] = high_price
                    self.w_data[symbol][str(i_sec)]['avg_qty'] = avg_qty
                    self.w_data[symbol][str(i_sec)]['total_qty'] = total_qty
                    self.w_data[symbol][str(i_sec)]['min_qty'] = min_qty
                    self.w_data[symbol][str(i_sec)]['max_qty'] = max_qty
                    self.w_data[symbol][str(i_sec)]['trades_count'] = trades_count
                    self.w_data[symbol][str(i_sec)]['turnover'] = turnover

            print(f"Save file: {self.w_actual_file[symbol]}")
            pickle.dump(self.w_data[symbol], open(self.w_actual_file[symbol], "wb"))

    def w_close_db(self):
        for sy in self.symbols:
            self.w_save_db(sy)

    def add_data(self, symbol, day_str, sec_str, data):
        self.w_load_db(symbol, day_str)

        sid = data['id']
        i_found = False
        for wd in self.w_data[symbol][sec_str]['all']:
            if sid == wd['id']:
                i_found = True
                break
        if not i_found:
            self.w_data[symbol][sec_str]['all'].append(data.copy())

    def get_path_db_name(self, symbol, day_str):
        db_name = symbol + "_" + day_str + ".pickle"
        return self.path_pickdata_refined + symbol + "/" + db_name


if __name__ == '__main__':
    rp = Refine_tickdata()
    file_list = rp.get_filelist_pickdata()
    all_files = False
    selected_symbol = "ALL"
    for fx, fl in enumerate(file_list):
        print("\r" + f"job ready: {round(fx / len(file_list), 2)}% ", end="")
        if fl not in rp.tick_data_done_files or all_files:
            symbol = rp.get_symbol(fl)
            if symbol == selected_symbol or selected_symbol == "ALL":
                dict_data = rp.get_dict_by_filename(fl)
                # print(dict_data)
                # sys.exit(0)
                # print(file_list[0], dict_data[0])
                # print(file_list[0], rt.unix_to_datetime(dict_data[0]['time']))
                for dd in dict_data:
                    # print(dd)
                    # time.sleep(1)
                    day_str = rp.get_day_str(dd['time'])
                    # print(day_str)
                    sec_str = str(rp.get_time_no_in_sec(dd['time']))
                    rp.add_data(symbol, day_str, sec_str, dd)
            rp.add_done_files(fl)

    rp.w_close_db()
