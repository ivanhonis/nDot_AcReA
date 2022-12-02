import time
import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

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


def wait_on_b(i):
    print(i)
    time.sleep(5)


def wait_on_a(i):
    print(i)
    time.sleep(5)



executor = ThreadPoolExecutor(max_workers=200)
inow = datetime.datetime.now()
for i in range(150):
    # executor.submit(wait_on_b(i))
    executor.submit(wait_on_a(i))
print(datetime.datetime.now() - inow)


# import asyncio
# from binance import AsyncClient, BinanceSocketManager, Client
# from binance.enums import *
# from binance.exceptions import BinanceAPIException
# import requests
#
# res_count = 1
#
# def start_asyc_websocket():
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(asyc_websocket())
#     loop.close()
#
#
# async def asyc_websocket():
#     global res_count
#
#     i_socket_list = ['!bookTicker']
#
#     client = await AsyncClient.create()
#     bm = BinanceSocketManager(client)
#     ts = bm.multiplex_socket(i_socket_list)
#
#     async with ts as tscm:
#         while True:
#             res_count += 1
#             res = await tscm.recv()
#             print(res)
#             for rd in res:
#                 print(rd)
#             for rs in res["data"]:
#                 print(res["data"][rs])
#
# start_asyc_websocket()