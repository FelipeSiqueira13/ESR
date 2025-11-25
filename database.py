import json
import sys

class DataBase():

    def __init__(self, name):

        # ----------- CARREGA CONFIG E VIZINHOS DO ARQUIVO 1 -----------
        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        # ip_to_viz = { ip : endereco }
        # agora mantendo o dicionário, não lista
        self.ip_to_viz = ip_config.get(name, {})

        print("Vizinhos carregados:", self.ip_to_viz)

        # ----------- FORMATO ANTIGO (arquivo 2) -----------
        # Agora vizinhos é a lista de endereços
        self.vizinhos = list(self.ip_to_viz.values())
        self.vizinhos_inicializados = {viz: 0 for viz in self.vizinhos}

        # tabelas de streams
        self.available_streams = []
        self.active_streams_table = {}
        self.streams_origin_table = {}

        # cria uma linha para cada vizinho
        for viz in self.vizinhos:
            self.active_streams_table[viz] = {}

    # =============================================================
    #                  FUNÇÕES DO PRIMEIRO ARQUIVO
    # =============================================================

    def get_vizinhos(self):
        """Retorna apenas os endereços"""
        return self.ip_to_viz.values()

    def get_my_ip(self, vizinho):
        """Dado um endereço, retorna o IP correspondente"""
        for ip, endereco in self.ip_to_viz.items():
            if endereco == vizinho:
                return ip
        return None

    # =============================================================
    #           FUNÇÕES DA TABELA DE STREAMS (arquivo 2)
    # =============================================================

    def addStream(self, stream_id):
        if stream_id not in self.available_streams:
            self.available_streams.append(stream_id)

            for viz in self.vizinhos:
                self.active_streams_table[viz][stream_id] = 0

            print(f"Added stream {stream_id}.\n")
        else:
            print(f"Stream {stream_id} already exists.\n")

    def activateStream(self, viz, stream_id):
        if stream_id in self.available_streams and viz in self.active_streams_table:
            self.active_streams_table[viz][stream_id] = 1
            print(f"Stream {stream_id} activated for neighbour {viz}.\n")
        else:
            print(f"ERROR: Stream {stream_id} cannot be activated for {viz}.\n")

    def deactivateStream(self, viz, stream_id):
        if stream_id in self.available_streams and viz in self.active_streams_table:
            self.active_streams_table[viz][stream_id] = 0
            print(f"Stream {stream_id} deactivated for neighbour {viz}.\n")
        else:
            print(f"ERROR: Stream {stream_id} cannot be deactivated for {viz}.\n")

    def printActiveStreamsTable(self):
        print("         " + " ".join(f"{s:>4}" for s in self.available_streams))
        for v in self.vizinhos:
            valores = " ".join(f"{self.active_streams_table[v][s]:>4}" for s in self.available_streams)
            print(f"{v:>4} {valores}")

    # =============================================================
    #           FUNÇÕES DE OVERLAY / VIZINHOS (arquivo 2)
    # =============================================================

    def inicializaVizinho(self, viz):
        if viz in self.vizinhos:
            self.vizinhos_inicializados[viz] = 1
            print(f"Neighbour {viz} initialized.\n")
        else:
            print(f"Neighbour {viz} could not be initialized.\n")

    def removeVizinho(self, viz):
        if viz in self.vizinhos:
            self.vizinhos_inicializados[viz] = 0
            print(f"Neighbour {viz} removed.\n")
        else:
            print(f"Neighbour {viz} could not be removed.\n")

    def printVizinhosInicializados(self):
        for viz, status in self.vizinhos_inicializados.items():
            print(f"{viz}: {status}")


# =============================================================
#                       MAIN PARA TESTE
# =============================================================
if __name__ == '__main__':
    if len(sys.argv) < 2:
        db = DataBase("R1")   # alterar depois no trabalho final
    else:
        db = DataBase(sys.argv[1])

    print(list(db.get_vizinhos()))
    print(db.get_my_ip(list(db.get_vizinhos())[0]))

    # testes antigos
    db.printActiveStreamsTable()
    db.addStream("s1")
    db.addStream("s2")
    db.addStream("s3")

    db.printActiveStreamsTable()

    db.activateStream("10.0.4.21", "s1")
    db.activateStream("10.0.4.20", "s2")
    db.activateStream("10.0.4.20", "s3")

    db.printActiveStreamsTable()
