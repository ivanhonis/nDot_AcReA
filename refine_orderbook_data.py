import os
import sys
import time
from os import listdir
from os.path import isfile, join
import pickle
import datetime
import psutil
import numpy as np
import gc
import hashlib

orig_orig = 0
orig_smoot = 0
smoot_orig = 0
nallowed = 0
all_datapoint = 0


class RefineOrderbook:
    orderbook_unpacked = []
    filelist_orderbook = []
    w_data = {}
    w_actual_file = {}
    r_data = {}
    r_actual_file = {}

    def __init__(self):
        # self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
        #            "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
        #            "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
        #            "THETAUSDT", "OMGUSDT"]

        # ATOMUSDT kétszer volt, LUNAUSDT kivettem mert csak a baj van vele
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT", "LUNAUSDT"]

        # OFF
        # del self.symbols[-1]
        # nincs elég memória csak a felét dolgozom fel
        # self.symbols = ["MANAUSDT"]

        self.path_orderbook = "X:/Apa/Coder/Binance_orderbook_history/2_series_2022_5_02/"
        # self.path_orderbook = "C:/coder/"
        self.path_orderbook_reorg = "X:/Apa/Coder/crypto_db_ndot/order_book/"
        # self.orderbook_unpacked = self.load_orderbook_unpacked()
        # self.filelist_orderbook = self.get_filelist_orderbook(30, 30)  # x hányas alkönyvtártól hányadikig
        self.w_data = self.get_defa_dict("dict")
        self.w_actual_file = self.get_defa_dict("")
        self.r_data = self.get_defa_dict("dict")
        self.r_actual_file = self.get_defa_dict("")
        self.overwrite_allowed = False
        # self.minus_counter = self.defa_minus_counter()
        # print(self.w_data, self.w_actual_file)
        # self.max_smoot_length = 0
        # self.chk_error = 0
        self.orderbook_data_done_files = []
        self.load_done_files()

    def load_done_files(self):
        try:
            self.orderbook_data_done_files = np.load("nDot_orderbook_data_done_files.npy")
        except:
            self.orderbook_data_done_files = []

    def save_done_files(self):
        np.save("nDot_orderbook_data_done_files", self.orderbook_data_done_files)

    def add_done_files(self, fn):
        fn = hashlib.md5(fn.encode('utf-8')).hexdigest()
        if fn not in self.orderbook_data_done_files:
            self.orderbook_data_done_files = np.append(self.orderbook_data_done_files, fn)

    def is_done_files(self, fn):
        fn = hashlib.md5(fn.encode('utf-8')).hexdigest()
        if fn in self.orderbook_data_done_files:
            return True
        else:
            return False

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
            print("save data", self.w_actual_file[symbol])
            newpath = self.path_orderbook_reorg + str(symbol).upper()
            if not os.path.exists(newpath):
                os.makedirs(newpath)

            pickle.dump(self.w_data[symbol], open(self.w_actual_file[symbol], "wb"))

    def w_close_db(self):
        for sy in self.symbols:
            self.w_save_db(sy)

    def add_data(self, symbol, day_str, sec_no, data, fname):
        global orig_orig, orig_smoot, nallowed, all_datapoint, smoot_orig

        self.w_load_db(symbol, day_str)
        if symbol in self.w_data and sec_no in self.w_data[symbol]:
            if self.w_data[symbol][sec_no]['data']['lastUpdateId'] != data['data']['lastUpdateId']:
                if self.w_data[symbol][sec_no]['datetime_valid'] == "data smooting" and \
                        data['datetime_valid'] == "orig":

                    log_str = f"{symbol}, {day_str}, {sec_no}, bug type: lastUpdateId incorrect: orig -> smooted"
                    print(log_str)
                    print(log_str, file=open('nDot_data_refine_log.txt', 'a'))
                    print("exist:" + str(self.w_data[symbol][sec_no]), file=open('nDot_data_refine_log.txt', 'a'))
                    print("tryed:" + str(data), file=open('nDot_data_refine_log.txt', 'a'))
                    orig_smoot += 1

                elif self.w_data[symbol][sec_no]['datetime_valid'] == "orig" and \
                        data['datetime_valid'] == "orig":

                    log_str = f"{symbol}, {day_str}, {sec_no}, bug type: lastUpdateId incorrect: orig -> orig"
                    print(log_str)
                    print(log_str, file=open('nDot_data_refine_log.txt', 'a'))
                    print("exist:" + str(self.w_data[symbol][sec_no]), file=open('nDot_data_refine_log.txt', 'a'))
                    print("tryed:" + str(data), file=open('nDot_data_refine_log.txt', 'a'))

                    orig_orig += 1

                elif self.w_data[symbol][sec_no]['datetime_valid'] == "orig" and \
                        data['datetime_valid'] == "data smooting":
                        # and \
                        # int(data['data']['lastUpdateId']) < int(self.w_data[symbol][sec_no]['data']['lastUpdateId']):

                    log_str = f"{symbol}, {day_str}, {sec_no}, bug type: lastUpdateId incorrect: smooted -> orig"
                    print(log_str)
                    print(log_str, file=open('nDot_data_refine_log.txt', 'a'))
                    print("exist:" + str(self.w_data[symbol][sec_no]), file=open('nDot_data_refine_log.txt', 'a'))
                    print("tryed:" + str(data), file=open('nDot_data_refine_log.txt', 'a'))
                    smoot_orig += 1

                else:
                    if self.overwrite_allowed:
                        self.w_data[symbol][sec_no] = data
                    else:
                        log_str = f"{symbol}, {day_str}, {sec_no}, bud type: Nan"
                        print(log_str)
                        print(log_str, file=open('nDot_data_refine_log.txt', 'a'))
                        print("exist:" + str(self.w_data[symbol][sec_no]), file=open('nDot_data_refine_log.txt', 'a'))
                        print("tryed:" + str(data), file=open('nDot_data_refine_log.txt', 'a'))
                        nallowed += 1
        else:
            self.w_data[symbol][sec_no] = data
            all_datapoint += 1

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

    def get_filelist_orderbook(self, ifrom=0, ito=25000):
        files = []
        for i in range(ifrom, ito + 1):
            mypath = self.path_orderbook
            mypath += str(i)
            if os.path.exists(mypath):
                # files += [f for f in listdir(mypath) if isfile(join(mypath, f))]
                for f in listdir(mypath):
                    if isfile(join(mypath, f)):
                        files.append(str(i) + "/" + f)
        files.sort(key=lambda x: int(''.join(filter(str.isdigit, x))))
        # ez azt csinálja, hogy A1 után A2 jön és nem A11
        return files

    # def save_orderbook_unpacked(self):
    #     pickle.dump(self.orderbook_unpacked, open("orderbook_unpacked.pickle", "wb"))
    #
    # def load_orderbook_unpacked(self):
    #     try:
    #         objectrep = open("orderbook_unpacked.pickle", "rb")
    #         return pickle.load(objectrep)
    #     except:
    #         return []

    # def set_orderbook_unpacked(self, file_name):
    #     if file_name not in self.orderbook_unpacked:
    #         self.orderbook_unpacked.append(file_name)
    #         self.save_orderbook_unpacked()

    def get_dict_by_filename(self, file_name):
        try:
            objectrep = open(self.path_orderbook + file_name, "rb")
            return pickle.load(objectrep)
        except:
            return []

    def get_dt_symbol(self, data):
        # print(data)
        symbol = data['stream'].split("@")[0].upper()
        dt = data['mod_datetime']
        day_str = (str(dt.year) + "_" + ("00" + str(dt.month))[-2:] + "_" + ("00" + str(dt.day))[-2:])
        rtime = datetime.time(dt.hour, dt.minute, dt.second)
        sec_no = (rtime.hour * 60 + rtime.minute) * 60 + rtime.second
        return symbol, day_str, rtime, sec_no

    def cut_msec(self, dt):
        #levágja a miliszekundumot
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    @staticmethod
    def get_symbol_data(dict_data, symbol):
        syb = symbol.lower() + "@depth20"
        symbol_dict = {}
        sy_no = 0
        for dd in dict_data:
            if dict_data[dd]['stream'] == syb:
                symbol_dict[sy_no] = dict_data[dd]
                sy_no += 1
        return symbol_dict

    def dict_data_refinery(self, dict_data):

        new_dict_data_symbol = {}
        ndd_id = 0
        for sy in self.symbols:
            # 1
            # szétszedem symbolokra
            symbol_dict = self.get_symbol_data(dict_data, sy)
            if symbol_dict and len(list(symbol_dict.keys())) > 5:
                ksd = list(symbol_dict.keys())
                ksd.reverse()

                # utolsótóol elkezdem kisimítani
                if self.cut_msec(symbol_dict[ksd[0]]['datetime']) > self.cut_msec(symbol_dict[ksd[1]]['datetime']):
                    symbol_dict[ksd[0]]['mod_datetime'] = self.cut_msec(symbol_dict[ksd[0]]['datetime'])
                    symbol_dict[ksd[0]]["datetime_valid"] = "orig"
                else:
                    # ha az első egyből egy összetorlódás, akkor keresem az első stabil pontoto és onnan vissza
                    # szűmolom a kezdő értéket
                    sxb = 0

                    for sxb in range((len(ksd) - 4)):
                        ilmo1 = self.cut_msec(symbol_dict[ksd[sxb]]['datetime'])
                        ilmo2 = self.cut_msec(symbol_dict[ksd[sxb + 1]]['datetime'])
                        ilmo3 = self.cut_msec(symbol_dict[ksd[sxb + 2]]['datetime'])
                        ilmo4 = self.cut_msec(symbol_dict[ksd[sxb + 3]]['datetime'])
                        a = str(ilmo1 - ilmo2)
                        b = str(ilmo2 - ilmo3)
                        c = str(ilmo3 - ilmo4)
                        d = a == "0:00:01" and b == "0:00:01" and c == "0:00:01"
                        if d:
                            break

                    ntime = self.cut_msec(symbol_dict[ksd[sxb]]['datetime'])
                    ntime = ntime + datetime.timedelta(seconds=sxb)
                    symbol_dict[ksd[0]]['mod_datetime'] = ntime
                    symbol_dict[ksd[0]]["datetime_valid"] = "orig"

                for sx in range((len(ksd) - 1)):
                    il = self.cut_msec(symbol_dict[ksd[sx]]['mod_datetime'])
                    ilmo = self.cut_msec(symbol_dict[ksd[sx + 1]]['datetime'])
                    if il <= ilmo:
                        symbol_dict[ksd[sx + 1]]['mod_datetime'] = il - datetime.timedelta(seconds=1)
                        symbol_dict[ksd[sx + 1]]["datetime_valid"] = "data smooting"
                    else:
                        symbol_dict[ksd[sx + 1]]['mod_datetime'] = self.cut_msec(symbol_dict[ksd[sx + 1]]['datetime'])
                        symbol_dict[ksd[sx + 1]]["datetime_valid"] = "orig"

                # for syd in symbol_dict:
                #     print(symbol_dict[syd]['data']['lastUpdateId'],
                #           symbol_dict[syd]['datetime'],
                #           symbol_dict[syd]['mod_datetime'],
                #           symbol_dict[syd]['datetime_valid']
                #           )

            new_dict_data_symbol[sy] = symbol_dict

        return new_dict_data_symbol

    # def chk_data(self, data_dict):
    #     for sy in self.symbols:
    #         symbol_dict = self.get_symbol_data(data_dict, sy)
    #         sy_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
    #         # sy_id_diff_set = set(self.get_id_diff(symbol_dict))
    #         # print("id diff set:", sy, sy_id_diff_set)
    #         if len(list(set(sy_diff))) == 1 and list(set(sy_diff))[0] == 1:
    #             pass
    #             # sy_kesy = list(symbol_dict.keys())
    #             # print(sy, symbol_dict[sy_kesy[0]]["mod_datetime"], symbol_dict[sy_kesy[-1]]["mod_datetime"], "Ready")
    #         else:
    #             self.chk_error += 1
    #             print('-' * 80)
    #             print(sy, "gond van")
    #             print(set(sy_diff))
    #             print('-' * 80)
    #             time.sleep(10)


if __name__ == '__main__':
    ro = RefineOrderbook()

    ro.filelist_orderbook = ro.get_filelist_orderbook(61, 61)  # x hányas alkönyvtártól hányadikig

    # 36- 45 ok
    # 46 - 50 ok
    # 51 - 55 ok
    # 56 - 60 ok

    # 1-12 ig volt egy adatgyűjtés és 13-35 újra kezdtem
    # ezért célszerű két menetben feldolgozni 1-12 és 13-35 hogy a szekvenciális feldolgozás gyorsabb legyen
    # 36- tól szintén

    all_files = False
    for i_file_name in ro.filelist_orderbook:
        if not ro.is_done_files(i_file_name) or all_files:
            # time_stamp_0 = datetime.datetime.now()
            print(datetime.datetime.now(), "read file:", i_file_name,
                  "orig_orig", orig_orig,
                  "orig_smoot", orig_smoot,
                  "smoot_orig", smoot_orig,
                  "nallowed", nallowed,
                  "all_datapoints", all_datapoint,
                  )
            dict_data = ro.get_dict_by_filename(i_file_name)
            # print("1", datetime.datetime.now() - time_stamp_0)
            # time_stamp_0 = datetime.datetime.now()

            dict_data_symbol = ro.dict_data_refinery(dict_data)
            # print("2", datetime.datetime.now() - time_stamp_0)
            # time_stamp_0 = datetime.datetime.now()

            for dict_data_s in dict_data_symbol:
                dict_data = dict_data_symbol[dict_data_s]
                if dict_data:
                    for dx, dd in enumerate(dict_data):
                        symbol, day_str, rtime, sec_no = ro.get_dt_symbol(dict_data[dd])
                        dict_data[dd]["source_file"] = i_file_name
                        ro.add_data(symbol, day_str, sec_no, dict_data[dd], dx)
            # print("3", datetime.datetime.now() - time_stamp_0)

            ro.add_done_files(i_file_name)

    ro.w_close_db()
    ro.save_done_files()
    reres = f"""{datetime.datetime.now()},
             orig_orig, {orig_orig},
             orig_smoot, {orig_smoot},
             smoot_orig, {smoot_orig},
             nallowed, {nallowed},
             all_datapoints, {all_datapoint}
             """
    print(str(reres), file=open('nDot_data_refine_results.txt', 'a'))



