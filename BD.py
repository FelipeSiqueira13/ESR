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

        self.ip_to_viz = ip_config.get(name, [])

        print(self.ip_to_viz)

    def get_vizinhos(self):
    
        return self.ip_to_viz.values()
    
    def get_my_ip(self, vizinho):
        for ip, endereco in self.ip_to_viz.items():
            if endereco == vizinho:
                return ip
        return None
    


if __name__ == '__main__':
    if len(sys.argv) < 2:  
        db = DataBase("R1")   #alterar isso no trabalho final para um erro
    else:
        db = DataBase(sys.argv[1])
    print(db.get_vizinhos())
    print(db.get_my_ip(list(db.get_vizinhos())[1]))