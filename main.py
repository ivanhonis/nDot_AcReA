import os
import sys
import time
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
        self.path_orderbook = "C:/coder/"
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
        #levágja a miliszekundumot
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    def chk_zero(self, array, pos, est_len):
        # ellenőrzi, hogy amennyivel nagyobb a lépés annyi marad ki előtte
        # ez az összetorlódott stream miatt kell
        # de előfordul, hogy nem összetorlódás van hanem simán kimaradt egy stream ezzel nem tudok mit tenni.
        l = len(array)
        zero_count = 0
        if pos + est_len <= l:
            for i in range(pos, pos + est_len):
                # print(array[i])
                if array[i] == 0:
                    zero_count += 1
            if est_len - 1 == zero_count:
                return True
            else:
                return False
        else:
            return False
    
    def replace_block(self, array, pos, est_len):
        # ha összetorlódás van akko újraszámozással kisimítja
        for i in range(pos, pos + est_len):
            array[i] = 1
        return array
    
    def get_mod_sec(self, array):
        # vissza adja a modosított diferenciált
        # a összetorlódást kisimítja
        # a lyukakat hagyja
        for i in range(len(array)):
            if array[i] > 1:
                if self.chk_zero(array, i, array[i]):
                    array = self.replace_block(array, i, array[i])
        return array

    def get_mod_datetime(self, start_dt, diff_array):
        # vissza adja a módosított idővektort
        # a módosított sec diff alapján újra építi a másodperceket
        dt_array = []
        c_dt = datetime.datetime(start_dt.year, start_dt.month, start_dt.day, start_dt.hour, start_dt.minute, start_dt.second)
        dt_array.append(c_dt)
        for i in range(len(diff_array)):
            n_dt = dt_array[i] + datetime.timedelta(seconds=diff_array[i])
            dt_array.append(n_dt)
        return dt_array
    
    def get_sec_diff(self, symbol_dict, dty="datetime"):
        # visszadja az eredeti sec differenciált
        # visszafelé
        dkeys = list(symbol_dict.keys())
        dkeys = dkeys[::-1]
        # symbol_dict_mod = {}
        sec_diff_orig = []
        for kid in range(len(dkeys) - 1):
            act = symbol_dict[dkeys[kid]]
            act_dt = dr.cut_msec(act[dty])
    
            next = symbol_dict[dkeys[kid + 1]]
            next_dt = dr.cut_msec(next[dty])
    
            dif_dt = act_dt - next_dt
            dif_df_int = int(str(dif_dt.seconds))
            sec_diff_orig.append(dif_df_int)
        sec_diff_orig = sec_diff_orig[::-1]
        return sec_diff_orig
    
    def get_symbol_data(self, dict_data, symbol):
        syb = symbol.lower() + "@depth20"
        symbol_dict = {}
        sy_no = 0
        for dd in dict_data:
            if dict_data[dd]['stream'] == syb:
                symbol_dict[sy_no] = dict_data[dd]
                sy_no += 1
        return symbol_dict
    
    def dict_data_refinery(self, dict_data):
        new_dict_data = {}
        ndd_id = 0
        for sy in dr.symbols:
            symbol_dict = dr.get_symbol_data(dict_data, sy)
            sec_diff_orig = dr.get_sec_diff(symbol_dict)
            
            sec_diff_mod = dr.get_mod_sec(sec_diff_orig.copy())
            start_dt = dr.cut_msec(symbol_dict[0]['datetime'])
            mod_dt = dr.get_mod_datetime(start_dt, sec_diff_mod)
        
            for x, syd in enumerate(symbol_dict):
                if self.cut_msec(symbol_dict[syd]['datetime']) == mod_dt[x]:
                    symbol_dict[syd]["mod_datetime"] = mod_dt[x]
                    symbol_dict[syd]["datetime_valid"] = "original"
                else:
                    symbol_dict[syd]["mod_datetime"] = mod_dt[x]
                    symbol_dict[syd]["datetime_valid"] = "modified - crowded"

            # sec_diff_mod_test = dr.get_sec_diff(symbol_dict, "mod_datetime")
            # for jx, jd in enumerate(symbol_dict):
            #     if sec_diff_mod_test[jx] == 0:
            #         print("-" * 80)
            #     print(jx, symbol_dict[jd]["mod_datetime"], sec_diff_orig[jx])


            ## innen megcsinálom a gap ek kitöltését az előzővel
            symbol_sec_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            exp_symbol_dict = {}
            esd_no = 0
            for ix, vx in enumerate(symbol_sec_diff):
                if vx == 1:
                    exp_symbol_dict[esd_no] = symbol_dict[ix].copy()
                    esd_no += 1
                else:
                    exp_symbol_dict[esd_no] = symbol_dict[ix].copy()
                    exp_symbol_dict[esd_no]["datetime_valid"] = "modified - silent start" + str(vx)
                    # print("megvolt")
                    # print(exp_symbol_dict[esd_no]['stream'],
                    #       exp_symbol_dict[esd_no]['datetime'],
                    #       exp_symbol_dict[esd_no]['mod_datetime'],
                    #       exp_symbol_dict[esd_no]['datetime_valid']
                    #       )
                    esd_no += 1
                    ibase_dt = self.cut_msec(symbol_dict[ix]['mod_datetime'])
                    ibase_data = symbol_dict[ix].copy()
                    for exx in range(vx-1):
                        nm_dt = ibase_dt + datetime.timedelta(seconds=exx + 1)
                        ibase_data['mod_datetime'] = nm_dt
                        ibase_data["datetime_valid"] = "modified - silent " + str(exx) + " - " + str(vx)
                        exp_symbol_dict[esd_no] = ibase_data.copy()
                        esd_no += 1
            
            for isyd in exp_symbol_dict:
                new_dict_data[ndd_id] = exp_symbol_dict[isyd]
                ndd_id += 1
        
        return new_dict_data


if __name__ == '__main__':
    dr = DataReorganizer()
    print(dr.orderbook_unpacked)
    dr.set_orderbook_unpacked("f2")
    print(dr.orderbook_unpacked)
    filelist_ob = dr.filelist_orderbook
    # print(filelist_ob)
    filelist_ob.sort(key=lambda x: int(''.join(filter(str.isdigit, x))))
    # print(filelist_ob)
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


    dict_data = dr.get_dict_by_filename("1/nDotBNC_A_2.pickle")
    new_dict_data = dr.dict_data_refinery(dict_data)
    
    for xvz, ssy in enumerate(new_dict_data):
        print(xvz,
              new_dict_data[ssy]['stream'],
              new_dict_data[ssy]['datetime'],
              new_dict_data[ssy]['mod_datetime'],
              new_dict_data[ssy]['datetime_valid']
              )
        
    
    # s_keys = list(new_dict_data.keys())
    #
    # for x in range(len(s_keys)-1):
    #     a = dr.cut_msec(new_dict_data[s_keys[x-1]]['mod_datetime'])
    #     b = dr.cut_msec(new_dict_data[s_keys[x]]['mod_datetime'])
    #
    #     dif_dt = b - a
    #     dif_df_int = int(str(dif_dt.seconds))
    #
    #     if dif_df_int != 1:
    #         print(a, b, dif_df_int)
            # print(dif_df_int,
            #       new_dict_data[s_keys[x - 1]]['stream'],
            #     new_dict_data[s_keys[x - 1]]['mod_datetime'],
            #       new_dict_data[s_keys[x]]['stream'],
            #       new_dict_data[s_keys[x]]['mod_datetime'])
    #
    # print("jön")
    

    
    # for x, ssy in enumerate(new_dict_data):
    #     print(new_dict_data[ssy]['datetime'], new_dict_data[ssy]['mod_datetime'], nsec_diff[x])
        
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
