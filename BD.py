import json
import sys

"""
preciso de algo para guardar o próprio endereço, a sofia n colocou, fiquei decepcionado
"""

class DataBase():

    def __init__(self, name):
        #self.vizinhos = {}

        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        self.vizinhos = ip_config.get(name, [])

        print(self.vizinhos)


if __name__ == '__main__':
    db = DataBase(sys.argv[1])