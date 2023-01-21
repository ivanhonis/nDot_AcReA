import paramiko
import os
import pickle
import numpy as np


class server_connection:

    def __init__(self):
        self.sever_name = "Vultr - nDot LOB"
        self.username = 'root'
        self.password = ''
        self.hostname = ''
        self.ports = 22
        # self.transport = ""
        # self.sftp = ""

    def log(self, text):
        print(text)
        # self.gui_log(text)

    def open_connect(self):
        self.log(f"Login. nDot_trade_server: {self.hostname}")
        self.transport = paramiko.Transport((self.hostname, self.ports))
        self.transport.connect(username=self.username, password=self.password)
        self.transport.default_window_size = 4294967294  # 2147483647
        self.transport.packetizer.REKEY_BYTES = pow(2, 40)
        self.transport.packetizer.REKEY_PACKETS = pow(2, 40)

        # self.transport.window_size = 3 * 1024 * 1024
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def close_connect(self):
        self.sftp.close()
        self.transport.close()
        self.log(f"Log Out nDot_trade_server")

    def put(self, remotepath, localpath):
        try:
            self.sftp.put(remotepath=remotepath, localpath=localpath)
        except BaseException as error:
            print(f'n_trade_server_connection -> put exception: {error}')

    def get(self, remotepath, localpath):
        try:
            self.sftp.get(remotepath=remotepath, localpath=localpath)

        except BaseException as error:
            print(f'n_trade_server_connection -> get exception: {error}')

    def get_fast(self, remotepath, localpath=""):
        ftp_file = self.sftp.file(remotepath, "r")
        ftp_file_size = ftp_file.stat().st_size
        ftp_file.prefetch(ftp_file_size)
        ftp_file.set_pipelined()
        ftp_file_data = ftp_file.read(ftp_file_size)
        print(ftp_file_data)
        # text_file = open("data.npz", "w")
        # n = text_file.write(ftp_file_data)
        # text_file.close()
        # a = np.load("data.npz")
        # print(a)

    def remove(self, remotepath):
        try:
            self.sftp.remove(path=remotepath)
        except BaseException as error:
            print(f'n_trade_server_connection -> get exception: {error}')

    def listdir(self, remotepath):
        try:
            i_return = self.sftp.listdir(path=remotepath)
            return i_return
        except BaseException as error:
            print(f'n_trade_server_connection -> listdir exception: {error}')
            return []


if __name__ == '__main__':
    tc = server_connection()
    tc.open_connect()
    print(tc.listdir("/root/"))
    tc.get("/root/bid_ask_spread_avg.npy", "./monitor_data/bid_ask_spread_avg.npy")
    tc.close_connect()
