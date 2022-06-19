import os
import sys
import time
from os import listdir
from os.path import isfile, join
import pickle
import datetime
import psutil
import gc


class Refine_pickdata:
    def __init__(self):
        self.symbols = ["ATOMUSDT", "BTCUSDT", "ETHUSDT", "NMRUSDT", "SANDUSDT", "SOLUSDT", "FTMUSDT", "XRPUSDT",
                   "LUNAUSDT", "MANAUSDT", "NEARUSDT", "AVAXUSDT", "TRXUSDT", "ROSEUSDT", "ONEUSDT", "ALGOUSDT",
                   "DOTUSDT", "VETUSDT", "LRCUSDT", "ETCUSDT", "LINKUSDT", "SHIBUSDT", "BCHUSDT",
                   "THETAUSDT", "OMGUSDT"]

        self.path_pickdata = "D:/Apa/Coder/Binance_tick_data/"
        self.path_pickdata_refined = "D:/Apa/Coder/crypto_db_ndot/tick_data/"
        self.filelist_orderbook = self.get_filelist_orderbook() #x hányas alkönyvtártól hányadikig

    def get_filelist_orderbook(self):
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

    def get_data_from_name(self, name):
        name = name.replace(".pickle", "")
        sep_array = name.split("-")
        return sep_array[0], sep_array[1], sep_array[2]


if __name__ == '__main__':
    rt = Refine_pickdata()
    file_list = rt.get_filelist_orderbook()
    print(rt.get_data_from_name(file_list[0]))
    dict_data = rt.get_dict_by_filename(file_list[0])
    print(file_list[0], dict_data[0])
    print(file_list[0], rt.unix_to_datetime(dict_data[0]['time']))
    for dd in dict_data:
        print(rt.unix_to_datetime(dd['time']), dd['id'], dd['price'])
    print(file_list[0], dict_data[0])