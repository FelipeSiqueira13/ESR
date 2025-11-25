import json
import sys
import threading

import socket

class DataBase():

    def __init__(self, name):

        with open('../config/server_config.json', 'r') as file:
            server_info = json.load(file)
        
        self.server_streams = server_info.get(name, {}).get("streams", {})
