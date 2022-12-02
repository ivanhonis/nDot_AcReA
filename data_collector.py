import asyncio
# import sys
import psutil
import pickle
import time
import os
import multiprocessing
from multiprocessing import Value, Manager
from google.cloud import storage
import gc
import datetime
import json
import socket

# Binanace
from binance import AsyncClient, BinanceSocketManager


class TimeGates:

    def __init__(self):
        self.time_dict = {}
        self.time_steps = 60 * 20  # idő váltások másodpercben
        self.get_time_dict()
        self.overlap = 10  # sec átfedés az időkapuk között, ezért majd tisztítani kell az adatokat

    def get_time_dict(self):
        a = datetime.datetime.now()
        x = int(a.minute)
        z = 1
        # a következő páros percben indítok, ez azért kell, hogy a két példány
        while (x + z) % 2 != 0:
            z += 1

        c = a + datetime.timedelta(minutes=z)
        c = datetime.datetime(c.year, c.month, c.day, c.hour, c.minute)
        # print(a, c)

        time_dict = {}
        time_dict[0] = c
        time_dict[1] = time_dict[0] + datetime.timedelta(seconds=self.time_steps)
        time_dict[2] = time_dict[1] + datetime.timedelta(seconds=self.time_steps)
        self.time_dict = time_dict

    def shift_time_dict(self):
        self.time_dict[0] = self.time_dict[2]
        self.time_dict[1] = self.time_dict[0] + datetime.timedelta(seconds=self.time_steps)
        self.time_dict[2] = self.time_dict[1] + datetime.timedelta(seconds=self.time_steps)

    def get_time_flag(self, pos):
        if pos == 0:
            if datetime.datetime.now() < self.time_dict[0]:
                return 0
            elif self.time_dict[0] < datetime.datetime.now() < self.time_dict[1] + datetime.timedelta(seconds=self.overlap):
                return 1
            elif self.time_dict[1] + datetime.timedelta(seconds=self.overlap) < datetime.datetime.now():
                return 2
        if pos == 1:
            if datetime.datetime.now() < self.time_dict[1]:
                return 0
            elif self.time_dict[1] < datetime.datetime.now() < self.time_dict[2] + datetime.timedelta(seconds=self.overlap):
                return 1
            elif self.time_dict[2] + datetime.timedelta(seconds=self.overlap) < datetime.datetime.now():
                return 2

class n_book_saver:

    def __init__(self):
        self.api_key = "DAqss9T987L0ruIbVEW9rBEFDD2sKxEKBvpvDVUJfdjijzqPqBgD8semkNF2I5Ul"
        self.api_secret = "3C1203CjVU3J0djfqG62QUSA2sFJJwWnHAmd7gd7t87OoOJJbx7NCnFV7PXx4Wpk"
        self.b_client = None
        self.account = self.get_account()
        self.exchange_info = self.get_exchange_info()
        self.time_pos = 0  # 0 első időkapu 1 második időkapun megy át
        self.block_name = "A" # ez teszi egyedivé minden inditáskor a file nevet mindíg cserélni kell indításkor
        self.tg = TimeGates()
        self.time_flag = 0
        self.chk_delay = 5  # sec
        # self.stream_dict = {}

        # self.stram_chanel = True
        self.stream_pos = 0
        # self.stream1_dict = {}
        # self.stream1_pos = 0
        # self.all_pos = 0
        self.last_save_name = ""
        self.proc = None
        self.min_memory_GB = 100000


        # self.symbols = ['AGLD', 'STPT', 'MXN', 'UGX', 'RENBTC', 'GLM', 'RAY', 'NEAR', 'AUDIO', 'HNT', 'ADADOWN', 'CDT', 'SPARTA', 'SUSD', 'FARM', 'XNO', 'AION', 'NPXS', 'DGB', 'ZRX', 'BCD', 'EASY', 'SANTOS', 'WING', 'WNXM', 'BCH', 'JST', 'ADAUP', 'HOT', 'AR', 'IRIS', 'RAMP', 'BCX', 'SEK', 'TRIG', 'RCN', 'COVER', 'FLM', 'GNO', 'VITE', 'GNT', 'BKRW', 'CFX', 'XPR', 'SFP', 'DIA', 'RDN', 'ACA', 'ARDR', 'LOOMOLD', 'NEBL', 'ACH', 'SLPOLD', 'BEL', 'JUV', 'ACM', 'MINA', 'GRTDOWN', 'VTHO', 'PYROLD', 'SGB', 'SALT', 'STORM', 'REN', 'REP', 'ADA', 'ELF', 'REQ', 'STORJ', 'CHF', 'ADD', 'BZRX', 'SGT', 'DF', 'RARE', 'EOSDOWN', 'PAXG', 'YOYO', 'PAX', 'CHR', 'VND', 'BCHDOWN', 'WAVES', 'CHZ', 'ADX', 'XRP', 'WPR', 'JASMY', 'AED', 'FIDA', 'SAND', 'DKK', 'OCEAN', 'FOR', 'UMA', 'DREPOLD', 'SCRT', 'TUSD', 'EZ', 'TKO', 'WABI', 'RGT', 'IDRT', 'ENG', 'ENJ', 'UNIDOWN', 'YFII', 'KZT', 'OAX', 'GRT', 'GRS', 'UND', 'HARD', 'TFUEL', 'ENS', 'LEND', 'DLT', 'TROY', 'XLMUP', 'UNI', 'BTCDOWN', 'TLM', 'HUF', 'SBTC', 'CKB', 'WRX', 'XTZ', 'LUNA', 'ETHDOWN', 'AGI', 'BCHA', 'EON', 'EOP', 'EOS', 'GO', 'NCASH', 'RIF', 'NSBT', 'SKL', 'XDATA', 'GTC', 'PEN', 'BLINK', 'SOLO', 'SXPDOWN', 'HC', 'SKY', 'BURGER', 'NAS', 'NAV', 'GTO', 'WTC', 'XVG', 'EPS', 'DNT', 'CLV', 'FLOW', 'XTZDOWN', 'XVS', 'STEEM', 'BVND', 'SLP', 'VRT', 'NBS', 'DON', 'LAZIO', 'DOT', 'IQ', 'GRTUP', '1INCH', 'KNCL', 'CHESS', 'MITH', 'ERD', 'DEGO', 'CND', 'GYEN', 'UNFI', 'FTM', 'POWR', 'ERN', 'GVT', 'WINGS', 'FTT', 'VOXEL', 'PHA', 'RLC', 'PHB', 'TRXDOWN', 'ATOM', 'XRPUP', 'QUICK', 'BLZ', 'SNM', 'BOBA', 'MBL', 'MTLX', 'SNT', 'PHP', 'SNX', 'LTCDOWN', 'FUN', 'SNMOLD', 'COP', 'COS', 'API3', 'USD', 'QKC', 'SUSHIUP', 'ROSE', 'GLMR', 'XYM', 'PURSE', 'SOL', 'TRXUP', 'CITY', 'ETC', 'BNC', 'CELR', 'UST', 'OGN', 'ETH', 'NEO', 'TOMO', 'CELO', 'KLAY', 'AUCTION', 'BADGER', 'HIGH', 'GXS', 'TRB', 'BNT', 'QLC', 'LBA', 'MDA', 'BNX', 'UTK', 'WSOL', 'HEGIC', 'MA', 'AMB', 'MC', 'TRU', 'FUEL', 'DREP', 'TRY', 'TRX', 'MDT', 'NFT', 'MDX', 'XRPDOWN', 'AERGO', 'EUR', 'AMP', 'BOT', 'NULS', 'AUTO', 'NGN', 'ANC', 'BDOT', 'EGLD', 'ANTOLD', 'SPELL', 'PUNDIX', 'FXS', 'PLA', 'HNST', 'EVX', 'CRV', 'BAKE', 'ANT', 'NU', 'FLUX', 'ANY', 'LINKUP', 'SRM', 'QISWAP', 'TORN', 'PLN', 'QNT', 'ALICE', 'OG', 'MFT', 'OM', 'BTTOLD', 'BETH', 'BQX', 'WETH', 'PHBV1', 'BETA', 'BRD', 'SSV', 'BUSD', 'CTK', 'ARPA', 'DOTDOWN', 'BRL', 'ALCX', 'CTR', 'MATIC', 'IOTX', 'SHIB', 'TVK', 'FRONT', 'ZAR', 'DOCK', 'STX', 'PNT', 'QI', 'DENT', 'MBOX', 'SUB', 'POA', 'IOST', 'CAKE', 'ETHUP', 'POE', 'OMG', 'BAND', 'SUN', 'ASTR', 'SUNOLD', 'BTC', 'TWT', 'NKN', 'RSR', 'IOTA', 'CVC', 'REEF', 'BTG', 'MIR', 'KES', 'ARK', 'LOKA', 'CVP', 'ARN', 'KEY', 'BTS', 'SPARTAOLD', 'ARS', 'CVX', 'ONE', 'LINKDOWN', 'ONG', 'ANKR', 'SUSHI', 'ALGO', 'SC', 'WBTC', 'ONT', 'PPT', 'ONX', 'BTTC', 'RUB', 'PIVX', 'ASR', 'FIRO', 'AXSOLD', 'AST', 'MANA', 'DOTUP', 'ATA', 'MEETONE', 'QSP', 'ATD', 'NMR', 'MKR', 'DODO', 'LIT', 'ICP', 'ZEC', 'ATM', 'APPC', 'JEX', 'ICX', 'LOOM', 'ZEN', 'KP3R', 'DOGE', 'DUSK', 'ALPHA', 'BOLT', 'SXP', 'HBAR', 'RVN', 'MLN', 'AUD', 'LTOOLD', 'IDR', 'CTSI', 'KAVA', 'C98', 'PSG', 'HCC', 'VIDT', 'NOK', 'AVA', 'SYS', 'COCOS', 'STRAX', 'EOSUP', 'CZK', 'GAS', 'COVEROLD', 'AAVEDOWN', 'THETA', 'BCHUP', 'WAN', 'ORN', 'PERL', 'XLMDOWN', 'MASK', 'AAVE', 'GBP', 'PERP', '1INCHUP', 'SXPUP', 'YFIDOWN', 'BOND', 'YFI', 'PERLOLD', 'MOD', 'BICO', 'OST', 'XEC', 'YGG', 'PEOPLE', 'AXS', 'ZIL', 'VAI', 'XEM', 'CTXC', 'KEYFI', 'XTZUP', 'BIDR', 'BCHSV', 'AAVEUP', 'SUSHIDOWN', 'COMP', 'ETHBNT', 'OMOLD', 'OOKI', 'RUNE', 'FORTH', 'KMD', 'GHST', 'IDEX', 'DEXE', 'AVAX', 'UAH', 'KNC', 'PROS', 'PROM', 'BTCUP', 'CHAT', 'BGBP', 'LPT', 'HIVE', 'BIFI', 'PORTO', 'SNGLS', 'PYR', 'WAXP', 'DAI', 'YFIUP', 'DAR', 'FET', 'LRC', 'REPV1', 'ADXOLD', 'MTH', 'MTL', 'VET', 'ALPACA', 'USDT', 'USDS', 'OXT', 'USDP', 'DASH', 'NVT', 'SWRV', 'EDO', 'ILV', 'GHS', 'BTCST', 'HKD', 'JOE', 'LSK', 'KEEP', 'CAD', 'BEAM', 'CAN', 'DCR', 'CREAM', 'DATA', 'IMX', 'ENTRP', 'FILUP', 'UNIUP', 'LTC', 'USDC', 'WIN', 'LTCUP', 'INJ', 'TCT', 'PARA', 'LTO', 'VGX', 'TRIBE', 'NXS', 'EFI', 'DYDX', 'AGIX', 'INR', 'CBK', 'CBM', 'INS', 'POND', 'JPY', 'LINA', 'XLM', 'LINK', 'QTUM', 'FILDOWN', 'SUPER', 'UFT', 'POLS', 'KSM', 'LUN', 'FIL', 'POLY', 'STMX', 'RNDR', 'BAL', 'FIO', 'GALA', 'VIB', 'VIA', 'FIS', 'BAR', 'RAD', 'BAT', 'VRAB', 'AKRO', 'NZD', 'MOVR', 'XMR', '1INCHDOWN', 'COTI']

        # self.symbols = ['BTC', 'ETH', 'ADA', 'LINK', 'DOT', 'TRX', 'FTM', 'SOL', 'BUSD',
        # 				'USDT', 'MATIC', 'ETC', 'NEO', 'ENJ', 'WAVES', 'ATOM', 'ONE', 'ZEC',
        # 				'ONT', 'HOT', 'CHZ', 'WIN', 'AXS', 'GALA', 'ANKR', 'RUNE', 'ICP', 'LRC',
        # 				'ZIL', 'BCHABC', 'TFUEL', 'ERD', 'DUSK', 'ARPA', 'EGLD', 'UNI', 'GRT', 'FIS',
        # 				'ALICE', 'NU', 'QTUM', 'ZRX', 'OMG', 'STRAT', 'IOTA', 'REP', 'ADX', 'NULS',
        # 				'DASH', 'POWR', 'XMR', 'BTS', 'XZC', 'LSK', 'LEND', 'ICX', 'AION', 'RLC',
        # 				'IOST', 'NANO', 'BLZ', 'SYS', 'XEM', 'TUSD', 'ZEN', 'SC', 'DENT', 'RVN',
        # 				'USDC', 'BCHSV', 'PHB', 'COCOS', 'TOMO', 'XTZ', 'WRX', 'CHR', 'STMX', 'YFI',
        # 				'SRM', 'KSM', 'SUSHI', 'BEL', 'NEAR', 'SLP', 'REEF', 'C98', 'MINA', 'VOXEL']

        # # USDT Setup -----------------------------------------------------------------
        self.symbols = ["ATOM", "BTC", "ETH", "NMR", "SAND", "SOL", "FTM", "XRP",
        					"LUNA", "MANA", "NEAR", "AVAX", "TRX", "ROSE", "ONE", "ALGO", "DOT", "VET",
        					"ATOM", "LRC", "ETC", "LINK", "SHIB", "BCH", "THETA", "OMG"]
        # 	self.quote_symbols = ["USDT"]
        # 	self.max_open_position = 10
        # 	self.stock_size = 800  # usd

        # USDT Setup -----------------------------------------------------------------
        # self.symbols = ['API3', 'ACH', 'ADA', 'ALGO', 'ALICE', 'ANT', 'ATOM', 'AVAX', 'AXS', 'BCH', 'BNB', 'BTC',
        #                 'CAKE', 'CFX', 'CHR', 'CHZ', 'COCOS', 'CRV', 'DAR', 'DOGE', 'DOT', 'DYDX', 'EGLD', 'INJ',
        #                 'ENJ', 'ENS', 'EOS', 'ETC', 'ETH', 'FIL', 'FTM', 'GALA', 'GLMR', 'HNT', 'ICP', 'IMX',
        #                 'JST', 'KAVA', 'LINK', 'LRC', 'LTC', 'LUNA', 'MANA', 'MATIC', 'MBOX', 'NEAR',
        #                 'ONE', 'ROSE', 'RUNE', 'SAND', 'SHIB', 'SOL', 'SUN', 'SUSHI', 'TFUEL', 'THETA', 'TLM',
        #                 'TRX', 'USDC', 'UST', 'VET', 'VOXEL', 'WIN', 'XRP', 'ZEC']

        self.quote_symbols = ["USDT"]


        # BTC SETUP --------------------------------------------------------------------
        # 	self.symbols = ['ADA', 'ATOM', 'AVAX', 'AXS', 'BNB', 'DOT', 'ENJ', 'ETH', 'FTM',
        # 					'GALA', 'LINK', 'LRC', 'LTC', 'LUNA', 'MANA', 'MATIC', 'NEAR',
        # 					'SAND', 'SOL', 'TFUEL', 'XRP']

        # OFF BNB

        self.all_pairs = self.defa_all_pairs()

        self.selected_symbols = self.symbols  ## kiválasztam amivel dolgozok
        self.selected_pairs = self.defa_selected_pairs()  ##a kiválasztott szimbólumokhoz kapcsolódó párokat kiválasztom
        self.socket_list = self.defa_socket_list()
        # self.pair_info = self.defa_pair_info()

    def update_time_flag(self):
        self.time_flag = self.tg.get_time_flag(self.time_pos)
        return self.time_flag

    def upload_to_bucket(self, path_to_file, blob_name, bucket_name="ndot2"):
        storage_client = storage.Client.from_service_account_json(
            'tribal-radar-284116-9bfd84d521e2.json')
        # print(path_to_file)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name + ".pickle")
        blob.upload_from_filename(path_to_file + ".pickle")
        return blob.public_url

    def del_file(self, f_name):
        if os.path.isfile(f_name + ".pickle"):
            os.remove(f_name + ".pickle")

    def get_file_size(self, f_name):
        stats = os.stat(f_name + ".pickle")
        return stats.st_size

    def get_free_mem(self):
        return psutil.virtual_memory().free / 1073741824  # return in GB

    def save_dict(self, sdict, f_name):
        # print(len(sdict))
        # print(len(str(sdict)))
        self.last_save_name = f_name
        pickle.dump(sdict, open(f_name + ".pickle", "wb"))

    def load_dict(self, name):
        # print(name)
        return pickle.load(open(name + ".pickle", "rb"))

    def save_status(self, status="Ping"):
        # print("Status write")
        act_memory_GB = psutil.virtual_memory().free / 1024 / 1024 / 1024
        self.min_memory_GB = min(self.min_memory_GB, act_memory_GB)
        status = {"status": status,
                  "date_time": datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"),
                  "act free_mem (GB)": act_memory_GB,
                  "min free_mem (GB)": self.min_memory_GB,
                  "last_save_name": self.last_save_name,
                  "time gates (Minute)": self.tg.time_steps / 60,
                  "time gates": str(self.tg.time_dict)}

        with open('status.txt', 'w') as outfile:
            json.dump(status, outfile, indent=2)

    def save_last_result(self, i_str):
        with open('last_result.txt', 'w') as outfile2:
            json.dump(i_str, outfile2)

    async def open_binance_client(self):
        self.b_client = await AsyncClient.create(self.api_key, self.api_secret)

    async def close_binance_client(self):
        await self.b_client.close_connection()
        del self.b_client

    async def _get_account(self):
        await self.open_binance_client()
        res = await self.b_client.get_account()
        await self.close_binance_client()
        return res

    def get_account(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._get_account())

    async def _get_exchange_info(self):
        await self.open_binance_client()
        res = await self.b_client.get_exchange_info()
        await self.close_binance_client()
        return res

    def get_exchange_info(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._get_exchange_info())

    def get_symbol_info2(self, symbol):
        for item in self.exchange_info['symbols']:
            if item['symbol'] == symbol.upper():
                return item
        return None

    def defa_all_pairs(self):
        i_all_pairs = []
        for sy in self.exchange_info['symbols']:
            i_all_pairs.append(sy['symbol'])
        return i_all_pairs

    def defa_socket_list(self):
        i_socket_list = []
        for sp in tuple(self.selected_pairs.keys()):
            i_socket_list.append(sp.lower() + '@depth20')
            # i_socket_list.append(sp.lower() + '@trade')
        return i_socket_list

    def defa_selected_pairs(self):
        i_selected_pairs = {}
        for si1 in self.selected_symbols:
            for si2 in self.quote_symbols:
                if si1 + si2 in self.all_pairs and \
                        self.get_symbol_info2(si1 + si2)['status'] == 'TRADING' and \
                        "MARKET" in self.get_symbol_info2(si1 + si2)['orderTypes']:
                    # print(si1 + si2)
                    i_selected_pairs[si1 + si2] = [si1, si2]
        return i_selected_pairs

    def n_start(self):

        global global_stream_num
        global global_result_dict

        self.proc = multiprocessing.Process(target=self.start_asyc_websocket, args=(global_stream_num, global_result_dict))
        self.proc.start()

    def n_stop(self):
        # print("terminate")
        # print(n_bs.time_flag)
        self.proc.join()
        time.sleep(6)
        self.proc.terminate()
        # print("terminate end")

    async def asyc_websocket(self, global_stream_num, global_result_dict):

        # print("asyc_websocket cx len", len(tuple(self.stream_dict.keys())))

        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)

        ts = bm.multiplex_socket(self.socket_list)
        async with ts as tscm:
            while True:
                # print("glob_num.value", global_stream_num.value)
                if global_stream_num.value == 1:
                    res = await tscm.recv()
                    # self.all_pos += 1
                    res['datetime'] = datetime.datetime.now()
                    global_result_dict[self.stream_pos] = res
                    self.stream_pos += 1
                    # print("in asyc_websocket cx len", len(tuple(self.stream_dict.keys())))
                    # print(res)
                    # glob_num.value = 0
                else:
                    # print("break")
                    break
        # print("close 1")
        await ts.__aexit__(None, None, None)
        # print("close 2")
        await client.close_connection()
        # print("close 3")
        del client
        # print("close 4")
        del bm
        # print("close 5")
        del ts

    def start_asyc_websocket(self, global_stream_num, global_result_dict):

        # print("glob_num.value", global_stream_num.value)

        self.stream_pos = 0
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.asyc_websocket(global_stream_num, global_result_dict))
        time.sleep(10)

        # loop.close() nem kell lezárni leáll magától


if __name__ == '__main__':
    host_name = socket.gethostname()

    # processzek között megosztott változók
    global_stream_num = Value('d', 1.0)  # 1 megy a stream 0 leáll
    manager = Manager()
    global_result_dict = manager.dict() ## ez pedig a result dict

    n_bs = n_book_saver()
    if host_name == "A" or host_name == "Aurora_R9":
        # print("A")
        n_bs.time_pos = 0
    else:
        # print("B")
        n_bs.time_pos = 1

    dt_tag = datetime.datetime.now().strftime("%m%d_%H%M")
    # sfile_name_prefix = "nDotBNC_" + dt_tag + "_" + str(n_bs.time_pos) + "_"
    sfile_name_prefix = "nDotBNC_" + str(n_bs.block_name) + "_"
    sfile_sufix = n_bs.time_pos

    n_bs.save_status()
    last_time_flag = n_bs.time_flag


    # status txt hány percenként készüljön el
    status_ping = 60  ## másodpercenként
    dt_now = datetime.datetime.now()

    # fname = "nDotBNC_0417_2018_0_0"
    # x = n_bs.load_dict(fname)
    # print("x len ", len(x))
    # print(x[0])
    # sys.exit(0)

    fast_chk = .25
    sleep_timer = fast_chk


    while True:

        t_flag = n_bs.update_time_flag()
        # print(t_flag, datetime.datetime.now(), global_stream_num.value)
        # if t_flag != 1:
        #     print(t_flag, datetime.datetime.now())

        if t_flag == 1 and last_time_flag == 0:
            sleep_timer = n_bs.chk_delay
            # print("Start", str(datetime.datetime.now()))
            last_time_flag = t_flag
            global_stream_num.value = 1
            # print("start stream")
            global_result_dict = manager.dict()
            n_bs.n_start()
        if t_flag == 2:
            sleep_timer = fast_chk
            # print("Stop", str(datetime.datetime.now()))
            global_stream_num.value = 0
            n_bs.n_stop()
            lkey = tuple(global_result_dict.keys())[-1]
            lresult = str(global_result_dict[lkey])
            n_bs.save_last_result(lresult)

            sdic = global_result_dict.copy()
            # print(sdic[0])
            # for gd in global_result_dict:
            #     sdic[gd] = global_result_dict[gd]

            # print(global_result_dict[0])
            fname = sfile_name_prefix + str(sfile_sufix)
            blob = sfile_name_prefix + str(sfile_sufix)
            sfile_sufix += 2
            n_bs.save_status("Save to disk")
            n_bs.save_dict(sdic, fname)

            n_bs.save_status("Upload")
            n_bs.upload_to_bucket(fname, blob)
            n_bs.del_file(fname)

            gc.collect()
            n_bs.tg.shift_time_dict()
            n_bs.update_time_flag()
            last_time_flag = n_bs.time_flag

        time.sleep(sleep_timer)

        if datetime.datetime.now() > dt_now + datetime.timedelta(seconds=status_ping):
            dt_now = datetime.datetime.now()
            n_bs.save_status("Ping")
