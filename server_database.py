import json
import sys
import threading

import socket

class DataBase():

    def __init__(self, name):

        with open('server_config.json', 'r') as file:
            server_info = json.load(file)
        
        self.server_streams = server_info.get(name, {}).get("streams", {})



    def get_streams(self):
        """Retorna lista de ids streams"""
        return list(self.server_streams.keys())


if __name__ == '__main__':
    if len(sys.argv) < 2:
        db = DataBase("R1")   # alterar depois no trabalho final
    else:
        db = DataBase(sys.argv[1])

    print(db.get_streams())