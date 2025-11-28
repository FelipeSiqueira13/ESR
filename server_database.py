import json
import sys
import threading
# from database import DataBase

import socket

class ServerDataBase():

    def __init__(self, name):

        with open('server_config.json', 'r') as file:
            server_info = json.load(file)
        
        self.server_streams = server_info.get(name, {}).get("streams", {})
        
        self.server_vizinhos = {}
        self.lock = threading.Lock()

        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        viz = ip_config.get(name, {})
        self.server_vizinhos = {}
        self.stream_vizinhos = {}

        for v in viz:
            self.server_vizinhos[v] = 0
            



    def get_streams(self):
        with self.lock:
            """Retorna lista de ids streams"""
            return list(self.server_streams.keys())

    def inicializaVizinho(self, ip):
        with self.lock:
            if ip in self.server_vizinhos:
                self.server_vizinhos[ip] = 1

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sdb = ServerDataBase("S1")   # alterar depois no trabalho final
    else:
        sdb = ServerDataBase(sys.argv[1])

    print(sdb.get_streams())