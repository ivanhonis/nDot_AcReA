import datetime
import pickle
import random
from datetime import datetime, timedelta
from statistics import mean
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

class nDot_db_connector:

    def __init__(self):
        self.orderbook_data = {}
        self.orderbook_actual_file = ""
        self.tickdata_data_store = {}
        self.tickdata_data = {}
        self.tickdata_actual_file = {}
        self.tickdata_files_in_memory = []
        self.tickdata_max_files_in = 4
        self.actual_work_file = ""
        self.path_orderbook = "X:/Apa/coder/crypto_db_ndot/order_book/"
        self.path_tickdata = "X:/Apa/coder/crypto_db_ndot/tick_data/"

        # ordebook now() és a helyi now()  között van egy óra eltérés
        # orderbook a vultrben keletkezik és adat érkezésekor kap egy időbélyeget
        # tick data helyileg kerül letöltésre és megérkezéskor a binance api helyi időre állítja át
        # azaz ha lekéred az urolsó tickdatát helyi pc-n illetve a vultr pc-n nyári téli időszámítástól
        # függően 1 illetve két óra lesz
        self.dt_sync_block = (60 * 60)  # ordebook now() és a helyi now()  között van eltérés
        self.dt_sync_winter1 = datetime.fromisoformat("2022-10-30 00:00:00")
        self.dt_sync_winter2 = datetime.fromisoformat("2022-10-30 01:00:00")

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
                # for ix in self.orderbook_data:
                #     print(ix, self.orderbook_data[ix])
                #     time.sleep(.1)
                # print(self.orderbook_data)
                self.orderbook_actual_file = work_file
            except:
                self.orderbook_data = {}
                self.orderbook_actual_file = ""

    def get_orderbook_data(self, symbol, idt):

        day_str = self.get_day_str_from_dt_orderbook(idt)
        sec_int = int(self.get_time_no_in_sec_from_dt(idt))

        if symbol in self.symbols:
            # print('itt')
            self.load_orderbook_db(symbol, day_str)
            if sec_int in self.orderbook_data:
                ret = {"bids": self.orderbook_data[sec_int]['data']['bids'],
                       "asks": self.orderbook_data[sec_int]['data']['asks'],
                       "lastUpdateId": self.orderbook_data[sec_int]['data']['bids'],
                       "datetime": self.orderbook_data[sec_int]['datetime'],
                       "mod_datetime": self.orderbook_data[sec_int]['mod_datetime'],
                       "datetime_valid": self.orderbook_data[sec_int]['datetime_valid'],
                       "source_file": self.orderbook_data[sec_int]['source_file'],
                       "stream": self.orderbook_data[sec_int]['stream']
                       }
                return ret
            else:
                return {}
        else:
            return {}

    def load_tickdata_db(self, symbol, day_str):
        work_file = self.get_db_name_tickdata(symbol, day_str)
        if work_file in self.tickdata_data_store:
            if self.actual_work_file != work_file:
                self.tickdata_data = self.tickdata_data_store[work_file]
                self.actual_work_file = work_file
        else:
            try:
                objectrep = open(work_file, "rb")
                self.tickdata_data_store[work_file] = pickle.load(objectrep)
                self.tickdata_data = self.tickdata_data_store[work_file]
                self.actual_work_file = work_file
                self.tickdata_files_in_memory.append(work_file)
                if len(self.tickdata_files_in_memory) > self.tickdata_max_files_in:
                    del self.tickdata_data_store[self.tickdata_files_in_memory[0]]
                    self.tickdata_files_in_memory = self.tickdata_files_in_memory[1:]
            except:
                self.tickdata_data = {}
                self.tickdata_actual_file = ""

    @staticmethod
    def get_day_str_from_dt(idt):
        return str(idt.year).zfill(4) + str(idt.month).zfill(2) + str(idt.day).zfill(2)

    @staticmethod
    def get_day_str_from_dt_orderbook(idt):
        return str(idt.year).zfill(4) + '_' + str(idt.month).zfill(2) + '_' + str(idt.day).zfill(2)

    @staticmethod
    def get_time_no_in_sec_from_dt(idt):
        return (int(idt.hour) * 60 * 60) + (int(idt.minute) * 60) + int(idt.second)

    def get_tick_data(self, symbol, idt, zshifter=0):
        shifter = 0
        if idt < self.dt_sync_winter1:
            shifter = (60 * 60) * 2
        elif self.dt_sync_winter1 <= idt < self.dt_sync_winter2:
            shifter = 999999
            # ezt a tartományt nem tudom megfejteni így itt nem adok resultot
        elif idt >= self.dt_sync_winter2:
            shifter = (60 * 60) * 1

        if shifter != 999999:
            idt = idt + timedelta(seconds=shifter)
            day_str = self.get_day_str_from_dt(idt)
            sec_str = str(self.get_time_no_in_sec_from_dt(idt))
            if symbol in self.symbols:
                self.load_tickdata_db(symbol, day_str)
                if sec_str in self.tickdata_data:
                    return self.tickdata_data[sec_str].copy()
                else:
                    return {}
            else:
                return {}
        else:
            return {}

    @staticmethod
    def unix_to_datetime(ts):
        ts = int(ts)
        return datetime.fromtimestamp(int(ts) / 1000)


if __name__ == '__main__':
    n_db = nDot_db_connector()

    # date_time_string = "2022-11-13 00:00:00"
    # dt = datetime.fromisoformat(date_time_string)
    #
    # for i in range(1):
    #     result_tick = n_db.get_tick_data("BTCUSDT", dt)
    #
    #     print(result_tick)
    #     if result_tick:
    #         # print(result_tick['all'])
    #         print(n_db.unix_to_datetime(result_tick['last']['time']))
    #         print(result_tick['last']['price'])
    #         # print(result_tick['first'])
    #         # print(result_tick['avg_price'])
    #         # print(result_tick['low_price'])
    #         # print(result_tick['high_price'])
    #         # print(result_tick['avg_qty'])
    #         # print(result_tick['total_qty'])
    #         # print(result_tick['min_qty'])
    #         # print(result_tick['max_qty'])
    #         # print(result_tick['trades_count'])
    #         # print(result_tick['turnover'])
    #
    #     dt = dt + timedelta(seconds=1)
    #
    # date_time_string_ob = "2022-11-13 00:00:00"
    # dt1 = datetime.fromisoformat(date_time_string_ob)
    #
    # for ix in range(1):
    #     result_orderbook = n_db.get_orderbook_data("BTCUSDT", dt1)
    #     print(result_orderbook['datetime'])
    #
    #     if result_orderbook:
    #         for d in range(1):
    #             print(result_orderbook['bids'][d][1],
    #                   result_orderbook['bids'][d][0], " - ",
    #                   result_orderbook['asks'][d][0],
    #                   result_orderbook['asks'][d][1])
    #
    #     dt1 = dt1 + timedelta(seconds=1)




    # for zs in range(-60 * 60 * 1, 60 * 60 * 1, 1):
    #     print("\r" + f"ready:{zs}", end="")
    #     date_time_string = "2022-10-30 00:20:00"
    #     dtx = datetime.fromisoformat(date_time_string)
    #     avg_range = []
    #     for i in range(0, 10, 1):
    #         result_tick = n_db.get_tick_data("BTCUSDT", dtx, zshifter=zs)
    #         result_orderbook = n_db.get_orderbook_data("BTCUSDT", dtx)
    #
    #         if result_tick and len(result_tick['all']) > 0 and result_orderbook:
    #             prt = float(result_tick['all'][-1]['price'])
    #             pob = (float(result_orderbook['bids'][0][0]) + float(result_orderbook['asks'][0][0])) / 2
    #             avg_range.append(prt - pob)
    #
    #         dtx = dtx + timedelta(seconds=60)
    #
    #     up_lim = 4
    #     down_lim = -4
    #
    #     if down_lim < mean(avg_range) < up_lim and \
    #             down_lim < min(avg_range) < up_lim and \
    #             down_lim < max(avg_range) < up_lim:
    #         print(" ")
    #         print(dtx, avg_range, zs)

    date_time_string = "2022-10-30 00:00:00"

    for c in range(100):
        zs = random.randint(-60 * 60 * 1 * 30, 60 * 60 * 24 * 30)

        zsa = []

        dtx = datetime.fromisoformat(date_time_string) + timedelta(seconds=zs)

        for i in range(0, 500, 1):
            result_tick = n_db.get_tick_data("BTCUSDT", dtx)
            result_orderbook = n_db.get_orderbook_data("BTCUSDT", dtx)

            if result_tick and len(result_tick['all']) > 0 and result_orderbook:
                prt = float(result_tick['all'][-1]['price'])
                pob = (float(result_orderbook['bids'][0][0]) + float(result_orderbook['asks'][0][0])) / 2

                # print(dtx, zs, prt - pob)
                zsa.append(prt - pob)

            dtx = dtx + timedelta(seconds=2)

        if len(zsa) > 0:
            fig = plt.plot(zsa)
            plt.show()
