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
                        "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                        "THETAUSDT", "OMGUSDT"]

        self.path_orderbook = "D:/Apa/Coder/Binance_orderbook_history/2_series_2022_5_02/"
        # self.path_orderbook = "C:/coder/"
        self.path_orderbook_reorg = "D:/Apa/Coder/crypto_db_ndot/order_book/"
        self.orderbook_unpacked = self.load_orderbook_unpacked()
        self.filelist_orderbook = self.get_filelist_orderbook(0, 12)  # x hányas alkönyvtártól hányadikig
        self.w_data = self.get_defa_dict("dict")
        self.w_actual_file = self.get_defa_dict("")
        self.r_data = self.get_defa_dict("dict")
        self.r_actual_file = self.get_defa_dict("")
        self.minus_counter = self.defa_minus_counter()
        print(self.w_data, self.w_actual_file)
        self.max_smoot_length = 0

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
        # levágja a miliszekundumot
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    # def chk_zero(self, array, pos, est_len):
    #     # ellenőrzi, hogy amennyivel nagyobb a lépés annyi marad ki előtte
    #     # ez az összetorlódott stream miatt kell
    #     # de előfordul, hogy nem összetorlódás van hanem simán kimaradt egy stream ezzel nem tudok mit tenni.
    #     l = len(array)
    #     zero_count = 0
    #     if pos + est_len <= l:
    #         for i in range(pos, pos + est_len):
    #             # print(array[i])
    #             if array[i] == 0:
    #                 zero_count += 1
    #         if est_len - 1 == zero_count:
    #             return True
    #         else:
    #             return False
    #     else:
    #         return False

    # def replace_block(self, array, pos, est_len):
    #     # ha összetorlódás van akko újraszámozással kisimítja
    #     for i in range(pos, pos + est_len):
    #         array[i] = 1
    #     return array

    # def get_mod_sec(self, array):
    #     # vissza adja a modosított diferenciált
    #     # a összetorlódást kisimítja
    #     # a lyukakat hagyja
    #     for i in range(len(array)):
    #         if array[i] > 1:
    #             if self.chk_zero(array, i, array[i]):
    #                 array = self.replace_block(array, i, array[i])
    #     return array

    # def get_mod_datetime(self, start_dt, diff_array):
    #     # vissza adja a módosított idővektort
    #     # a módosított sec diff alapján újra építi a másodperceket
    #     dt_array = []
    #     c_dt = datetime.datetime(start_dt.year, start_dt.month, start_dt.day, start_dt.hour, start_dt.minute, start_dt.second)
    #     dt_array.append(c_dt)
    #     for i in range(len(diff_array)):
    #         n_dt = dt_array[i] + datetime.timedelta(seconds=diff_array[i])
    #         dt_array.append(n_dt)
    #     return dt_array

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
            i_new_sec_diff_test = dr.get_sec_diff(datadict, "datetime")
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
                          datadict[ssy]['datetime'],
                          datadict[ssy]['mod_datetime'],
                          datadict[ssy]['datetime_valid'],
                          i_new_sec_diff_test[xvz]
                          )
                else:
                    if 'datetime_valid' in datadict[ssy].keys():
                        print(symbol, prefix, xvz,
                              datadict[ssy]['stream'],
                              datadict[ssy]['datetime'],
                              datadict[ssy]['datetime_valid'],
                              i_new_sec_diff_test[xvz]
                              )
                    else:
                        print(symbol, prefix, xvz,
                              datadict[ssy]['stream'],
                              datadict[ssy]['datetime'],
                              i_new_sec_diff_test[xvz]
                              )

    def get_smoot_out_array(self, symbol_dict):
        i_return = []
        symbol_diff_orig = dr.get_sec_diff(symbol_dict)
        sy_keys = list(symbol_dict.keys())
        sdfx = 0
        while sdfx < len(symbol_diff_orig) - 1:
            sdf = symbol_diff_orig[sdfx]
            if sdf > 1:
                # print(sdfx, symbol_dict[sy_keys[sdfx]]['datetime'])
                # symbol_dict[sy_keys[sdfx]]["datetime_valid"] = "strat run out"
                # print("for run", sdfx, len(symbol_diff_orig) - sdfx - 1, len(symbol_diff_orig))
                for rund_out_x in range(1, len(symbol_diff_orig) - sdfx - 1):
                    run_out_dt = self.cut_msec(symbol_dict[sy_keys[sdfx]]["datetime"]) + datetime.timedelta(
                        seconds=rund_out_x)
                    if self.cut_msec(symbol_dict[sy_keys[sdfx + rund_out_x]]["datetime"]) == run_out_dt:
                        from_peaces_arrray = [sdfx, rund_out_x]
                        # symbol_dict[sy_keys[sdfx]]["datetime_valid"] = "strat run out" + str(rund_out_x)
                        # print("run out ok")
                        i_return.append(from_peaces_arrray)
                        sdfx = sdfx + rund_out_x
                        break
            sdfx += 1
        return i_return

    def dict_data_refinery(self, dict_data):

        new_dict_data = {}
        ndd_id = 0
        for sy in self.symbols:
            # 1111111111111111111111111111111111111111
            # szétszedem symbolokra

            symbol_dict = self.get_symbol_data(dict_data, sy)

            # 2
            # smooting ha eltolódás van rendbeteszem

            # 1B  Running out
            refector_dict = self.get_smoot_out_array(symbol_dict)
            sy_keys = list(symbol_dict.keys())
            for rd in refector_dict:
                if rd[1] > 5:
                    self.max_smoot_length = max(self.max_smoot_length, rd[1])
                    print("length of smooting", rd[1], self.max_smoot_length)
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

            # 22222222222222222222222222222222222222222222222
            # az összetorlódást kezelem

            # sec_diff_orig = dr.get_sec_diff(symbol_dict)
            # sec_diff_mod = dr.get_mod_sec(sec_diff_orig.copy())
            # start_dt = dr.cut_msec(symbol_dict[0]['datetime'])
            # mod_dt = dr.get_mod_datetime(start_dt, sec_diff_mod)
            #
            # for x, syd in enumerate(symbol_dict):
            #     if self.cut_msec(symbol_dict[syd]['datetime']) == mod_dt[x]:
            #         symbol_dict[syd]["mod_datetime"] = mod_dt[x]
            #         symbol_dict[syd]["datetime_valid"] = "original"
            #     else:
            #         print("crowded")
            # symbol_dict[syd]["mod_datetime"] = mod_dt[x]
            # symbol_dict[syd]["datetime_valid"] = "modified - crowded"

            # self.status_print("after crowded", sy, symbol_dict, False)

            # 3333333333333333333333333333333333333333333333333333
            # összetorlódás és csend (GAP) egyben
            # ebben esetben a szünetben lévő elemeket a szünet elejére sorolom

            # symbol_sec_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            #
            # ikey_sdic = list(symbol_dict.keys())
            # for ix, i_dat in enumerate(symbol_sec_diff):
            #     if i_dat == 0:
            #         symbol_dict[ikey_sdic[ix]]['mod_datetime'] = symbol_dict[ikey_sdic[ix-1]]['mod_datetime'] + datetime.timedelta(seconds=1)
            #         symbol_dict[ikey_sdic[ix]]["datetime_valid"] = "modified - silece in crowded refactor"
            #         print("silece in crowded refactor")

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
                    for exx in range(vx - 1):
                        nm_dt = ibase_dt + datetime.timedelta(seconds=exx + 1)
                        ibase_data['mod_datetime'] = nm_dt
                        ibase_data["datetime_valid"] = "modified - silent "
                        exp_symbol_dict[esd_no] = ibase_data.copy()
                        esd_no += 1

            # if sy == "ATOMUSDT":
            # self.status_print("after gap fill", sy, exp_symbol_dict, full=False, silent=True)

            # 555555555555555555555555555555555555555555555
            # backshift

            # print("modified - silent", sy, len(list(exp_symbol_dict.keys())))

            # test_diff = self.get_sec_diff(exp_symbol_dict, "mod_datetime")
            # test_diff.append(-1000)
            # if min(list(set(test_diff))) < 0:
            # print(sy, set(test_diff), len(list(exp_symbol_dict.keys())))

            # kiszed
            # ex_keys = list(exp_symbol_dict.keys())
            # for tx, t_diff in enumerate(test_diff):
            #     if t_diff == -1:
            # print("back shift")
            # exp_symbol_dict[ex_keys[tx]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx + 1]]["mod_datetime"] - datetime.timedelta(seconds=1)
            # exp_symbol_dict[ex_keys[tx]]["datetime_valid"] = "modified - back shift - 1"
            # exp_symbol_dict[ex_keys[tx - 1]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx]]["mod_datetime"] - datetime.timedelta(seconds=1)
            # exp_symbol_dict[ex_keys[tx - 1]]["datetime_valid"] = "modified - back shift - 2"
            # exp_symbol_dict[ex_keys[tx - 2]]["mod_datetime"] = exp_symbol_dict[ex_keys[tx - 1]]["mod_datetime"] - datetime.timedelta(seconds=1)
            # exp_symbol_dict[ex_keys[tx - 2]]["datetime_valid"] = "modified - back shift - 3"
            # del exp_symbol_dict[ex_keys[tx - 3]]
            # del exp_symbol_dict[ex_keys[tx - 4]]

            # if sy == "NMRUSDT":
            #     self.status_print("after back shift", sy, exp_symbol_dict.copy(), full=True)
            #     sy_diff = self.get_sec_diff(exp_symbol_dict, "mod_datetime")
            #     print(set(sy_diff))

            for isyd in exp_symbol_dict:
                new_dict_data[ndd_id] = exp_symbol_dict[isyd]
                ndd_id += 1

        return new_dict_data

    def chk_data(self, data_dict):
        for sy in self.symbols:
            symbol_dict = self.get_symbol_data(data_dict, sy)
            sy_diff = self.get_sec_diff(symbol_dict, "mod_datetime")
            if len(list(set(sy_diff))) == 1 and list(set(sy_diff))[0] == 1:
                sy_kesy = list(symbol_dict.keys())
                print(sy, symbol_dict[sy_kesy[0]]["mod_datetime"], symbol_dict[sy_kesy[-1]]["mod_datetime"], "Ready")
            else:
                print('-' * 80)
                print(sy, "gond van")
                print(set(sy_diff))
                print('-' * 80)
                time.sleep(10)


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

    # dict_data = dr.get_dict_by_filename("1/nDotBNC_A_0.pickle")
    # new_dict_data = dr.dict_data_refinery(dict_data)
    # dr.chk_data(new_dict_data)

    # for nky in dr.minus_counter:
    #     if dr.minus_counter[nky] > 0:
    #         print(nky, ":", dr.minus_counter[nky])

    for i_file_names in filelist_ob:
        print(i_file_names)
        dict_data = dr.get_dict_by_filename(i_file_names)
        new_dict_data = dr.dict_data_refinery(dict_data)
        # dr.chk_data(new_dict_data)

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
