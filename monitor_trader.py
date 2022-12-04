import os
import sys
import time
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import matplotlib.pyplot as plt
# import pyautoguil
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', '{:20,.2f}'.format)
pd.set_option('display.max_colwidth', None)
while True:
	objectrep = open("paper_trade_multy_status.pickle", "rb")
	tres = pickle.load(objectrep)
	tres_df = tres
	del tres_df['history_profit']
	df = pd.DataFrame(tres)
	# print(tres['history_profit'])
	print(datetime.now())
	print(df)
	# print("    monitor_fee:", tres['monitor_fee'])
	# print(" monitor_profit:", tres['monitor_profit'])
	# print("   monitor_stop:", tres['monitor_stop'])
	# print("   monitor_take:", tres['monitor_take'])
	# print("monitor_trailer:", tres['monitor_trailer'])
	time.sleep(30)
