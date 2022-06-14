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
        self.filelist_orderbook = self.get_filelist_orderbook(0, 12) #x hányas alkönyvtártól hányadikig
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

    # def get_sec_diff(self, symbol_dict, dty="datetime"):
    #     # visszadja az eredeti sec differenciált
    #     # visszafelé
    #     dkeys = list(symbol_dict.keys())
    #     dkeys = dkeys[::-1]
    #     # symbol_dict_mod = {}
    #     sec_diff_orig = []
    #     for kid in range(len(dkeys) - 1):
    #         act = symbol_dict[dkeys[kid]]
    #         act_dt = self.cut_msec(act[dty])
    #
    #         next = symbol_dict[dkeys[kid + 1]]
    #         next_dt = self.cut_msec(next[dty])
    #
    #         # print(act_dt, next_dt)
    #         dif_dt = act_dt - next_dt
    #         dif_df_int = int(str(dif_dt.seconds))
    #         # if dif_df_int > 5000:
    #         #     print('segg', dif_df_int, dty)
    #         #     print("segg dif", act_dt, next_dt,  act_dt - next_dt)
    #         #     print(act)
    #         #     print(next)
    #         #     time.sleep(15)
    #         sec_diff_orig.append(dif_df_int)
    #     sec_diff_orig = sec_diff_orig[::-1]
    #     return sec_diff_orig
    
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
            # 1111111111111111111111111111111111111111
            # szétszedem symbolokra

            symbol_dict = dr.get_symbol_data(dict_data, sy)

            # new_sec_diff_test = dr.get_sec_diff(symbol_dict, "datetime")
            # print(sy, set(new_sec_diff_test))

            # if sy == "SOLUSDT":
            #     for xvz, ssy in enumerate(symbol_dict):
            #         if symbol_dict[ssy]['stream'] == "manausdt@depth20":
            #             # if new_sec_diff_test[xvz] == 0:
            #             #     print("-" * 80)
            #             print(xvz,
            #                   symbol_dict[ssy]['stream'],
            #                   symbol_dict[ssy]['datetime'],
            #                   new_sec_diff_test[xvz]
            #                   # dict_data[ssy]['mod_datetime'],
            #                   # dict_data[ssy]['datetime_valid']
            #                   )

            # 22222222222222222222222222222222222222222222222
            # az összetorlódást kezelem

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

            # 3333333333333333333333333333333333333333333333333333
            # összetorlódás és csend (GAP) egyben
            # ebben esetben a szünetben lévő elemeket a szünet elejére sorolom

            # new_sec_diff_test = dr.get_sec_diff(symbol_dict, "mod_datetime")
            # new_sec_diff_test.append(-100)
            # if sy == "ATOMUSDT":
            #     for xvz, ssy in enumerate(symbol_dict):
            #         if new_sec_diff_test[xvz] != 1:
            #             print("előtte" + "-" * 80)
            #         print(xvz,
            #               symbol_dict[ssy]['stream'],
            #               symbol_dict[ssy]['datetime'],
            #               symbol_dict[ssy]['mod_datetime'],
            #               symbol_dict[ssy]['datetime_valid'],
            #               new_sec_diff_test[xvz]
            #               )


            symbol_sec_diff = self.get_sec_diff(symbol_dict, "mod_datetime")

            ikey_sdic = list(symbol_dict.keys())
            ibs_data = symbol_dict[ikey_sdic[0]].copy()
            for ix, i_dat in enumerate(symbol_sec_diff):
                if i_dat == 0:
                    symbol_dict[ikey_sdic[ix]]['mod_datetime'] = symbol_dict[ikey_sdic[ix-1]]['mod_datetime'] + datetime.timedelta(seconds=1)
                    symbol_dict[ikey_sdic[ix]]["datetime_valid"] = "modified - silece in crowded refactor"

            # new_sec_diff_test = dr.get_sec_diff(symbol_dict, "mod_datetime")
            # new_sec_diff_test.append(-100)
            # if sy == "ATOMUSDT":
            #     for xvz, ssy in enumerate(symbol_dict):
            #         if new_sec_diff_test[xvz] != 1:
            #             print("utána" + "-" * 80)
            #         print(xvz,
            #               symbol_dict[ssy]['stream'],
            #               symbol_dict[ssy]['datetime'],
            #               symbol_dict[ssy]['mod_datetime'],
            #               symbol_dict[ssy]['datetime_valid'],
            #               new_sec_diff_test[xvz]
            #               )


            # for xvz, ssy in enumerate(symbol_dict):
            #     if symbol_dict[ssy]['stream'] == "nmrusdt@depth20":
            #         if symbol_sec_diff[xvz] == 0:
            #             print("-" * 80)
            #         print(xvz,
            #               symbol_dict[ssy]['stream'],
            #               symbol_dict[ssy]['datetime'],
            #               symbol_dict[ssy]['mod_datetime'],
            #               symbol_dict[ssy]['datetime_valid'],
            #               symbol_sec_diff[xvz]
            #               )

            # symbol_sec_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            # symbol_sec_diff.append(-10000)
            # print(sy, "symbol_sec_diff xxxxx", list(set(symbol_sec_diff)))

            # unique_dict = {}
            # for udie in symbol_sec_diff:
            #     unique_dict[udie] = 1
            # print(unique_dict)

            # if sy == "ATOMUSDT":
            #     print(symbol_sec_diff)
            #     print(sy, "symbol_sec_diff xxxxx", set(symbol_sec_diff))
            #     for kx, kg in enumerate(symbol_sec_diff):
            #         if kg == list(set(symbol_sec_diff))[3]:
            #             print("ccccc-------", kx)
            #
            #
            #     time.sleep(5)
            #     for xvz, rtz in enumerate(symbol_dict):
            #         print(xvz,
            #               symbol_dict[rtz]['stream'],
            #               symbol_dict[rtz]['datetime'],
            #               symbol_dict[rtz]['mod_datetime'],
            #               symbol_dict[rtz]['datetime_valid'],
            #               symbol_sec_diff[xvz]
            #               )

            exp_symbol_dict = {}
            esd_no = 0
            for ix, vx in enumerate(symbol_sec_diff):
                if vx == 1:
                    exp_symbol_dict[esd_no] = symbol_dict[ix].copy()
                    esd_no += 1
                else:
                    # print("VX ----------------- ", vx)
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



            # print("modified - silent", sy, len(list(exp_symbol_dict.keys())))

            test_diff = self.get_sec_diff(exp_symbol_dict, "mod_datetime")
            # test_diff.append(-1000)
            if 0 in set(test_diff):
                # print(sy, set(test_diff), len(list(exp_symbol_dict.keys())))

                # kiszed
                ex_keys = list(exp_symbol_dict.keys())
                for tx, t_diff in enumerate(test_diff):
                    if t_diff == 0:
                        exp_symbol_dict[ex_keys[tx]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx]]["mod_datetime"] - datetime.timedelta(seconds=1)
                        exp_symbol_dict[ex_keys[tx]]["datetime_valid"] = "modified - back shift"
                        exp_symbol_dict[ex_keys[tx - 1]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx - 1]]["mod_datetime"] - datetime.timedelta(seconds=1)
                        exp_symbol_dict[ex_keys[tx]]["datetime_valid"] = "modified - back shift"
                        exp_symbol_dict[ex_keys[tx - 2]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx - 2]]["mod_datetime"] - datetime.timedelta(seconds=1)
                        exp_symbol_dict[ex_keys[tx]]["datetime_valid"] = "modified - back shift"
                        del exp_symbol_dict[ex_keys[tx - 3]]



                # test_diff = self.get_sec_diff(exp_symbol_dict, "mod_datetime")
                # print("after backshift: ", sy, set(test_diff), len(list(exp_symbol_dict.keys())))

                # for xvz, ssy in enumerate(exp_symbol_dict):
                #     if test_diff[xvz] == 0:
                #         print("-" * 80)
                #     print(xvz,
                #       exp_symbol_dict[ssy]['stream'],
                #       exp_symbol_dict[ssy]['datetime'],
                #       exp_symbol_dict[ssy]['mod_datetime'],
                #       test_diff[xvz])
                # time.sleep(2)

            # print("dict len: ", sy, len(list(exp_symbol_dict.keys())))
            test_diff = self.get_sec_diff(exp_symbol_dict, "mod_datetime")
            print(sy, set(test_diff))


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

    dict_data = dr.get_dict_by_filename("1/nDotBNC_A_8.pickle")
    # for xvz, ssy in enumerate(dict_data):
    #     if dict_data[ssy]['stream'] == "btcusdt@depth20":
    #         print(xvz,
    #               dict_data[ssy]['stream'],
    #               dict_data[ssy]['datetime'],
    #               )
    # #

    new_dict_data = dr.dict_data_refinery(dict_data)

    # for i_file_names in filelist_ob:
    #     print(i_file_names)
    #     dict_data = dr.get_dict_by_filename(i_file_names)
    #     new_dict_data = dr.dict_data_refinery(dict_data)

    new_sec_diff_test = dr.get_sec_diff(new_dict_data, "mod_datetime")
    print(set(new_sec_diff_test))

    #
    # for xvz, ssy in enumerate(new_dict_data):
    #     if new_dict_data[ssy]['stream'] == "btcusdt@depth20":
    #         if new_sec_diff_test[xvz] == 0:
    #             print("-" * 80)
    #         print(xvz,
    #               new_dict_data[ssy]['stream'],
    #               new_dict_data[ssy]['datetime'],
    #               new_dict_data[ssy]['mod_datetime'],
    #               new_dict_data[ssy]['datetime_valid'],
    #               new_sec_diff_test[xvz]
    #               )
    # #
    
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

            # base_dt = symbol_dict[0]['datetime']
            # print(base_dt)
            #
            # base_dt = datetime.datetime(base_dt.year, base_dt.month, base_dt.day, base_dt.hour,
            #                             base_dt.minute, base_dt.second)
            #
            # print(base_dt)
            #
            # for nd in symbol_dict:
            #     mod_dt = base_dt + datetime.timedelta(seconds=nd)
            #     print(nd, symbol_dict[nd]['stream'], symbol_dict[nd]['datetime'], mod_dt)
            #
            #
            #
            #
            # l_sec_no = 0
            # for fo in filelist_ob[0:200]:
            #     data_dict = dr.get_dict_by_filename(fo)
            #     # print(data_dict[0])
            #     for dd in data_dict:
            #         # print(data_dict[dd])
            #         symbol, day_str, rtime, sec_no = dr.get_dt_symbol(data_dict[dd])
            #         if symbol == 'BTCUSDT':
            #             if sec_no - 1 != l_sec_no:
            #                 print(symbol, day_str, rtime, sec_no, fo)
            #             l_sec_no = sec_no
            #                 # dr.add_data(symbol, day_str, sec_no, data_dict[dd])
            #             # print(len(str(data_dict[dd])))
            #     print("-" * 80)
            # # dr.w_close_db()
            #
            # # for i in range(90000):
            # #     d = dr.get_data("BTCUSDT", "2022_05_02", i)
            # #     if not d:
            # #         print(i)
