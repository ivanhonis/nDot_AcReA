import os
from os import listdir
from os.path import isfile, join
import pickle
import datetime

class DataReorganizer:
    orderbook_unpacked = []
    filelist_orderbook = []

    def __init__(self):
        self.path_orderbook = "D:/Apa/Coder/Binance_orderbook_history/2_series_2022_5_02/"
        self.orderbook_unpacked = self.load_orderbook_unpacked()
        self.filelist_orderbook = self.get_filelist_orderbook()

    def get_filelist_orderbook(self):
        files = []
        for i in range(1000):
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
        rday = (str(dt.year) + "_" + ("00" + str(dt.month))[-2:] + "_" + ("00" + str(dt.day))[-2:])
        rtime = datetime.time(dt.hour, dt.minute, dt.second)
        return symbol, rday, rtime,


if __name__ == '__main__':
    dr = DataReorganizer()
    print(dr.orderbook_unpacked)
    dr.set_orderbook_unpacked("f2")
    print(dr.orderbook_unpacked)
    filelist_ob = dr.filelist_orderbook
    print(filelist_ob)
    data_dict = dr.get_dict_by_filename(filelist_ob[0])
    print(data_dict[0])
    for dd in data_dict:
        # print(data_dict[dd])
        if dr.get_dt_symbol(data_dict[dd])[0] == 'ETHUSDT':
            print(dr.get_dt_symbol(data_dict[dd]))
            print(len(str(data_dict[dd])))

    # ttime1 = datetime.time(1, 0, 10)
    # ttime2 = datetime.time(0, 0, 15)
    # print(ttime1, ttime2)
    # print(ttime1 < ttime2)
