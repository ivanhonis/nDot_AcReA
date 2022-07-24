import os
import sys
import time
from os import listdir
from os.path import isfile, join
import pickle
import datetime
import psutil
import gc

class Refine_Orderbook:
    orderbook_unpacked = []
    filelist_orderbook = []
    w_data = {}
    w_actual_file = {}
    r_data = {}
    r_actual_file = {}

    def __init__(self):
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT"]

        # ATOMUSDT kétszer volt, LUNAUSDT kivettem mert csak a baj van vele
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT"]

        # nincs elég memória csak a felét dolgozom fel
        self.symbols = ["TRXUSDT"]

        self.path_orderbook = "D:/Apa/Coder/Binance_orderbook_history/2_series_2022_5_02/"
        # self.path_orderbook = "C:/coder/"
        self.path_orderbook_reorg = "D:/Apa/Coder/crypto_db_ndot/order_book/"
        self.orderbook_unpacked = self.load_orderbook_unpacked()
        self.filelist_orderbook = self.get_filelist_orderbook(30, 30) #x hányas alkönyvtártól hányadikig
        self.w_data = self.get_defa_dict("dict")
        self.w_actual_file = self.get_defa_dict("")
        self.r_data = self.get_defa_dict("dict")
        self.r_actual_file = self.get_defa_dict("")
        self.minus_counter = self.defa_minus_counter()
        print(self.w_data, self.w_actual_file)
        self.max_smoot_length = 0
        self.chk_error = 0
        self.orderbook_data_done_files = []
        self.load_done_files()

    def load_done_files(self):
        try:
            objectrep = open("orderbook_data_done_files.pickle", "rb")
            self.orderbook_data_done_files = pickle.load(objectrep)
        except:
            self.orderbook_data_done_files = []

    def save_done_files(self):
        pickle.dump(self.orderbook_data_done_files, open("orderbook_data_done_files.pickle", "wb"))

    def add_done_files(self, fn):
        if fn not in self.orderbook_data_done_files:
            self.orderbook_data_done_files.append(fn)
            self.save_done_files()

    def print_free_mem(self):
        print(round(psutil.virtual_memory().free / 1024 / 1024 / 1027, 2), "GB")

    def defa_minus_counter(self):
        i_ret = {}
        i_ret[0] = 0
        for i in range(60000):
            i_ret[0 - i] = 0
            i_ret[i] = 0
        return i_ret

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
            print("-" * 80)
            print("save data", self.w_actual_file[symbol])
            print("-" * 80)
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
        #levágja a miliszekundumot
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    def get_id_diff(self, symbol_dict):
        i_dkeys = list(symbol_dict.keys())
        i_id_diff_result = []
        for i_kid in range(len(i_dkeys) - 1):
            i_act = int(symbol_dict[i_dkeys[i_kid]]['data']['lastUpdateId'])
            i_next = int(symbol_dict[i_dkeys[i_kid + 1]]['data']['lastUpdateId'])
            i_id_diff_result.append(i_next - i_act)
        return i_id_diff_result

    def get_sec_diff(self, symbol_dict, diff_date_type="datetime"):
        i_dkeys = list(symbol_dict.keys())
        i_sec_diff_result = []
        for i_kid in range(len(i_dkeys) - 1):
            i_act = symbol_dict[i_dkeys[i_kid]]  ## alktuális adat
            i_act_dt = self.cut_msec(i_act[diff_date_type])  ## aktuális dateteime

            i_next = symbol_dict[i_dkeys[i_kid + 1]]  ## következő adat
            i_next_dt = self.cut_msec(i_next[diff_date_type])  ## következő datetime

            # print(act_dt, next_dt)
            if i_act_dt > i_next_dt:
                i_dif_dt = i_act_dt - i_next_dt  ## különbözet időben kifejezve
                i_dif_df_int = -int(str(i_dif_dt.seconds))
            else:
                i_dif_dt = i_next_dt - i_act_dt  ## különbözet időben kifejezve
                i_dif_df_int = int(str(i_dif_dt.seconds))
            i_sec_diff_result.append(i_dif_df_int)
        return i_sec_diff_result

    def get_symbol_data(self, dict_data, symbol):
        syb = symbol.lower() + "@depth20"
        symbol_dict = {}
        sy_no = 0
        for dd in dict_data:
            if dict_data[dd]['stream'] == syb:
                symbol_dict[sy_no] = dict_data[dd]
                sy_no += 1
        return symbol_dict

    def status_print(self, prefix, symbol, datadict, full=True, silent=False):
        if 'mod_datetime' in datadict[0].keys():
            i_new_sec_diff_test = self.get_sec_diff(datadict, "mod_datetime")
            i_nsdiff_set = list(set(i_new_sec_diff_test))
            # if (len(i_nsdiff_set) != 1 or i_nsdiff_set[0] != 1) and just_not_one:
            if not silent:
                print(symbol, prefix, i_nsdiff_set, "=> mod_datetime")
            for bx in i_nsdiff_set:
                self.minus_counter[bx] += 1
        else:
            i_new_sec_diff_test = ro.get_sec_diff(datadict, "datetime")
            i_nsdiff_set = list(set(i_new_sec_diff_test))
            # if (len(i_nsdiff_set) != 1 or i_nsdiff_set[0] != 1) and just_not_one:
            if not silent:
                print(symbol, prefix, i_nsdiff_set, "=> datetime")
            for bx in i_nsdiff_set:
                self.minus_counter[bx] += 1
        if full and not silent:
            i_new_sec_diff_test.append(-1000)
            for xvz, ssy in enumerate(datadict):
                if i_new_sec_diff_test[xvz] != 1:
                    print("-" * 80)
                if 'mod_datetime' in datadict[ssy].keys():
                    print(symbol, prefix, xvz,
                          datadict[ssy]['stream'],
                          datadict[ssy]['data']['lastUpdateId'],
                          datadict[ssy]['datetime'],
                          datadict[ssy]['mod_datetime'],
                          datadict[ssy]['data']['lastUpdateId'],
                          datadict[ssy]['datetime_valid'],
                          i_new_sec_diff_test[xvz]
                          )
                else:
                    if 'datetime_valid' in datadict[ssy].keys():
                        print(symbol, prefix, xvz,
                              datadict[ssy]['stream'],
                              datadict[ssy]['data']['lastUpdateId'],
                              datadict[ssy]['datetime'],
                              datadict[ssy]['datetime_valid'],
                              i_new_sec_diff_test[xvz]
                              )
                    else:
                        print(symbol, prefix, xvz,
                              datadict[ssy]['stream'],
                              datadict[ssy]['data']['lastUpdateId'],
                              datadict[ssy]['datetime'],
                              i_new_sec_diff_test[xvz]
                              )
                        
    def get_smoot_out_array(self, symbol_dict):
        # vissza adja azokat a szakaszokat ahol az elemek az idő újra rendezésével
        # azaz 1 másodperces léptetésével kisimul az adat set

        i_return = []
        symbol_diff_orig = ro.get_sec_diff(symbol_dict)
        sy_keys = list(symbol_dict.keys())
        sdfx = 0
        while sdfx < len(symbol_diff_orig) - 1:
            sdf = symbol_diff_orig[sdfx]
            if sdf > 1:
                for rund_out_x in range(1, len(symbol_diff_orig) - sdfx - 1):
                    run_out_dt = self.cut_msec(symbol_dict[sy_keys[sdfx]]["datetime"]) + datetime.timedelta(
                        seconds=rund_out_x)
                    if self.cut_msec(symbol_dict[sy_keys[sdfx + rund_out_x]]["datetime"]) == run_out_dt:
                        from_peaces_arrray = [sdfx, rund_out_x]
                        i_return.append(from_peaces_arrray)
                        sdfx = sdfx + rund_out_x
                        break
            sdfx += 1
        return i_return

    def dict_data_refinery(self, dict_data):

        new_dict_data = {}
        ndd_id = 0
        for sy in self.symbols:
            # 1
            # szétszedem symbolokra

            symbol_dict = self.get_symbol_data(dict_data, sy)

            # if sy == "ETHUSDT":
            #     self.status_print("data chk", sy, symbol_dict, full=True)

            # 2
            # smooting ha eltolódás van rendbeteszem
            
            refector_dict = self.get_smoot_out_array(symbol_dict)
            sy_keys = list(symbol_dict.keys())
            for rd in refector_dict:
                if rd[1] > 5:
                    self.max_smoot_length = max(self.max_smoot_length, rd[1])
                    # print("length of smooting", rd[1], self.max_smoot_length)
                shift_sec = 1
                smoot_base_dt = self.cut_msec(symbol_dict[sy_keys[rd[0]]]['datetime'])
                for rx in range(rd[0] + 1, rd[0] + rd[1]):
                    symbol_dict[sy_keys[rx]]["mod_datetime"] = smoot_base_dt + datetime.timedelta(seconds=shift_sec)
                    shift_sec += 1
                    symbol_dict[sy_keys[rx]]["datetime_valid"] = "modified - smooting"
                
                # self.status_print("start symboldict", sy, symbol_dict, full=True)
            #     time.sleep(10)

            # 2b
            # minden ami nem módosult átkerült orig státusszal és ugyan azzal az adattal az mod_datetime be
            # innentől a mod_data tartalmazza a teljes javított idősort
            
            for x, syd in enumerate(symbol_dict):
                if "mod_datetime" not in symbol_dict[syd].keys():
                    symbol_dict[syd]["mod_datetime"] = self.cut_msec(symbol_dict[syd]["datetime"])
                    symbol_dict[syd]["datetime_valid"] = "original"

            # 444444444444444444444444444444444444444444444444444444444444
            # adásszüneteket kitölti

            # if sy == "ATOMUSDT":
            # self.status_print("before gap fill", sy, symbol_dict, False)

            symbol_sec_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            exp_symbol_dict = {}
            esd_no = 0
            for ix, vx in enumerate(symbol_sec_diff):
                if vx == 1:
                    exp_symbol_dict[esd_no] = symbol_dict[ix].copy()
                    esd_no += 1
                elif vx > 1:
                    exp_symbol_dict[esd_no] = symbol_dict[ix].copy()
                    exp_symbol_dict[esd_no]["datetime_valid"] = "modified - silent start: " + str(vx - 1)
                    esd_no += 1
                    ibase_dt = self.cut_msec(symbol_dict[ix]['mod_datetime'])
                    ibase_data = symbol_dict[ix].copy()
                    for exx in range(vx-1):
                        nm_dt = ibase_dt + datetime.timedelta(seconds=exx + 1)
                        ibase_data['mod_datetime'] = nm_dt
                        ibase_data["datetime_valid"] = "modified - silent "
                        exp_symbol_dict[esd_no] = ibase_data.copy()
                        esd_no += 1

            # if sy == "ETHUSDT":
            #     self.status_print("data chk", sy, exp_symbol_dict, full=True)

            for isyd in exp_symbol_dict:
                new_dict_data[ndd_id] = exp_symbol_dict[isyd]
                ndd_id += 1
        
        return new_dict_data

    def chk_data(self, data_dict):
        for sy in self.symbols:
            symbol_dict = self.get_symbol_data(data_dict, sy)
            sy_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            # sy_id_diff_set = set(self.get_id_diff(symbol_dict))
            # print("id diff set:", sy, sy_id_diff_set)
            if len(list(set(sy_diff))) == 1 and list(set(sy_diff))[0] == 1:
                pass
                # sy_kesy = list(symbol_dict.keys())
                # print(sy, symbol_dict[sy_kesy[0]]["mod_datetime"], symbol_dict[sy_kesy[-1]]["mod_datetime"], "Ready")
            else:
                self.chk_error += 1
                print('-' * 80)
                print(sy, "gond van")
                print(set(sy_diff))
                print('-' * 80)
                time.sleep(10)


if __name__ == '__main__':
    ro = Refine_Orderbook()
    # dict_data = dr.get_dict_by_filename("30/nDotBNC_A_1858.pickle")
    # new_dict_data = dr.dict_data_refinery(dict_data)
    # dr.chk_data(new_dict_data)

    ro.filelist_orderbook = ro.get_filelist_orderbook(1, 12)  # x hányas alkönyvtártól hányadikig
    # 1-12 ig volt egy adatgyűjtés és 13-35 újra kezdtem
    # ezért célszerű két menetben feldolgozni 1-12 és 13-35 hogy a szekvenciális feldolgozás gyorsabb legyen
    # 36- tól szintén

    all_files = True
    for i_file_name in ro.filelist_orderbook:
        if i_file_name not in ro.orderbook_data_done_files or all_files:
            time_stamp_0 = datetime.datetime.now()
            # print(i_file_name, "max smoot length", ro.max_smoot_length, "chk error", ro.chk_error)
            print(datetime.datetime.now(), "read file")
            # gc.collect()
            # ro.print_free_mem()
            dict_data = ro.get_dict_by_filename(i_file_name)
            # print(datetime.datetime.now(), "refine")
            # ro.print_free_mem()
            dict_data = ro.dict_data_refinery(dict_data)
            # print(datetime.datetime.now(), "chk data")
            # ro.print_free_mem()
            ro.chk_data(dict_data)
            # print(datetime.datetime.now(), "start writeing")
            # ro.print_free_mem()
            if ro.chk_error == 0:
                for dd in dict_data:
                    symbol, day_str, rtime, sec_no = ro.get_dt_symbol(dict_data[dd])
                    # if symbol == 'BTCUSDT':
                    #     # print(symbol, day_str, rtime, sec_no)
                    # print("add data", symbol, day_str, sec_no)
                    ro.add_data(symbol, day_str, sec_no, dict_data[dd])
            # print("run time: ", i_file_name,  datetime.datetime.now() - time_stamp_0)
            ro.add_done_files(i_file_name)
            ro.print_free_mem()

    ro.w_close_db()
    # for i in range(90000):
    #     d = dr.get_data("BTCUSDT", "2022_05_02", i)
    #     if not d:
    #         print(i)
