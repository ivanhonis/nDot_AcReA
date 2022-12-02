# import time
# import datetime
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
#
# def sleep_secs(seconds):
#   time.sleep(seconds)
#   print(f'{seconds} has been processed')
#
# secs_list = [3,3, 3, 3, 3, 3]
#
# inow = datetime.datetime.now()
# with ThreadPoolExecutor() as executor:
#   results = executor.map(sleep_secs, secs_list)
# print(datetime.datetime.now() - inow)

import asyncio
import datetime
import random
import time

from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
from binance.exceptions import BinanceAPIException
import requests
from threading import Thread

from multiprocessing import shared_memory, Lock, Pool, cpu_count
lock = Lock()

res_count = 1
ax = [""] * 2000

def start_asyc_websocket1():

    def monitor():
        time_intervall = 10
        time_count = 0
        time.sleep(.015)
        while True:
            # start_datetime = datetime.datetime.now()
            save_rescount = res_count
            time.sleep(time_intervall)
            time_count += time_intervall
            print("    1", res_count - save_rescount, round(res_count / time_count, 8), "   ")

    monitor = Thread(target=monitor, args=[])
    monitor.start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyc_websocket1())
    loop.close()

def start_asyc_websocket2():

    def monitor():
        time_intervall = 10
        time_count = 0
        time.sleep(.035)
        while True:
            # start_datetime = datetime.datetime.now()
            save_rescount = res_count
            time.sleep(time_intervall)
            time_count += time_intervall
            print("    2", res_count - save_rescount, round(res_count / time_count, 8), "    ")

    monitor = Thread(target=monitor, args=[])
    monitor.start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyc_websocket2())
    loop.close()

def start_asyc_websocket3():

    def monitor():
        time_intervall = 10
        time_count = 0
        time.sleep(.065)
        while True:
            # start_datetime = datetime.datetime.now()
            save_rescount = res_count
            time.sleep(time_intervall)
            time_count += time_intervall
            print("    3", res_count - save_rescount, round(res_count / time_count, 8), "    ")

    monitor = Thread(target=monitor, args=[])
    monitor.start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyc_websocket3())
    loop.close()


async def asyc_websocket1():
    global res_count, ax

    i_socket_list = ['!bookTicker']

    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.multiplex_socket(i_socket_list)

    async with ts as tscm:
        while True:
            res_count += 1
            res = await tscm.recv()
            # await data_organizer(res)
            for rd in res:
                ax[random.randint(1, 1000)] = res[rd]
            for rs in res["data"]:
                ax[random.randint(1, 1000)] = res["data"][rs]

async def asyc_websocket2():
    global res_count, ax

    i_socket_list = ['!bookTicker']

    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.multiplex_socket(i_socket_list)

    async with ts as tscm:
        while True:
            res_count += 1
            res = await tscm.recv()
            await data_organizer(res)
            # for rd in res:
            #     ax[random.randint(1, 1000)] = res[rd]
            # for rs in res["data"]:
            #     ax[random.randint(1, 1000)] = res["data"][rs]

async def asyc_websocket3():
    global res_count, ax

    i_socket_list = ['!bookTicker']

    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.multiplex_socket(i_socket_list)

    async with ts as tscm:
        while True:
            res_count += 1
            res = await tscm.recv()
            dtatamanth = Thread(target=data_organizer3, args=[res])
            dtatamanth.start()


async def data_organizer(res):
    global ax
    for rd in res:
        ax[random.randint(1, 1000)] = res[rd]
    for rs in res["data"]:
        ax[random.randint(1, 1000)] = res["data"][rs]


def data_organizer3(res):
    global ax
    for rd in res:
        ax[random.randint(1, 1000)] = res[rd]
    for rs in res["data"]:
        ax[random.randint(1, 1000)] = res["data"][rs]


def start_test(param):
    core = param[0]
    if core == 1:
        print("test1 - streight")
        start_asyc_websocket1()
    if core == 2:
        print("test2 - async")
        start_asyc_websocket2()
    if core == 3:
        print("test3 - multy thread")
        start_asyc_websocket3()

if __name__ == '__main__':
    used_cores = 3
    params = [[1], [2], [3]]
    xpool = Pool(used_cores)
    res = xpool.map(start_test, params)
