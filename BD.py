import json

class DataBase:

    def __init__(self, name):
        #self.vizinhos = {}

        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        vizinhos = ip_config.get(name, [])

        print(self.vizinhos)


if __name__ == '__main__':
    db = DataBase()