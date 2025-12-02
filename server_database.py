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
        self.stream_vizinhos = {}
        self.lock = threading.Lock()

        with open('config.json', 'r') as file:
            ip_config = json.load(file)
    def initiate_stream(self,src,stream_id):
        with self.lock:
            """Adiciona o stream_id ao dicionario de streams_vizinhos"""
            if stream_id not in self.stream_vizinhos:
                self.stream_vizinhos[stream_id] = []
            self.stream_vizinhos[stream_id].append(src)
    
    def end_stream(self,src,stream_id):
        with self.lock:
            """Remove o stream_id do dicionario de streams_vizinhos"""
            if stream_id in self.stream_vizinhos:
                if src in self.stream_vizinhos[stream_id]:
                    self.stream_vizinhos[stream_id].remove(src)
                    if not self.stream_vizinhos[stream_id]:
                        del self.stream_vizinhos[stream_id]
    

    def get_my_ip(self, vizinho_ip):
        with self.lock:
            """Retorna o proprio ip do servidor"""
            for ip in self.server_vizinhos.keys():
                if ip == vizinho_ip:
                    return ip
            return None


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
        print(sdb.initiate_stream("10.0.0.1","stream1"))
        print(sdb.stream_vizinhos)
        print(sdb.end_stream("10.0.0.1","stream1"))
        print(sdb.stream_vizinhos)

        print(sdb.get_streams())
    else:
        sdb = ServerDataBase(sys.argv[1])
