from asyncio import streams
import json
import sys
import msg

import threading

class DataBase():

    def __init__(self, name):

        # ----------- CARREGA CONFIG E VIZINHOS DO ARQUIVO 1 -----------
        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        # agora os valores podem ser listas; normaliza para dict ip -> [viz1, viz2, ...]
        raw = ip_config.get(name, {})
        self.ip_to_viz = {ip: (v if isinstance(v, list) else [v]) for ip, v in raw.items()}
        self.lock = threading.Lock()

        print("Initialized neighbours (ip -> [neighbours]):", self.ip_to_viz)

        # cria lista plana de vizinhos e um mapa viz -> ip para busca reversa
        self.vizinhos = []
        self.viz_to_ip = {}
        for ip, neighs in self.ip_to_viz.items():
            for n in neighs:
                self.vizinhos.append(n)
                self.viz_to_ip[n] = ip

        self.vizinhos_inicializados = {viz: 0 for viz in self.vizinhos}

        # tabelas de streams
        self.available_streams = []
        self.active_streams_table = {}
        self.streams_origin_table = {}
        self.streams_metrics = {}

        # cria uma linha para cada vizinho
        for viz in self.vizinhos:
            self.active_streams_table[viz] = {}
            self.streams_metrics[viz] = {}

    def get_vizinhos(self):
        """Retorna apenas os endereços"""
        return list(self.vizinhos)

    def get_my_ip(self, vizinho):
        """Dado um endereço, retorna o IP correspondente"""
        # usa o mapa invertido para suportar múltiplos vizinhos por IP
        return self.viz_to_ip.get(vizinho)

    def addStream(self, stream_id, origin_ip):
        with self.lock:
            if stream_id not in self.available_streams:
                self.available_streams.append(stream_id)
                self.streams_origin_table[stream_id] = origin_ip

                for viz in self.vizinhos:
                    self.active_streams_table[viz][stream_id] = 0
                    self.streams_metrics[viz][stream_id] = 0

                print(f"Added stream {stream_id}.\n")
            else:
                print(f"Stream {stream_id} already exists.\n")

    def AtualizaMetricas(self, viz, streams_id, metric):
        with self.lock:
            for stream in streams_id:
                self.streams_metrics[viz][stream] = metric*0.15 + self.streams_metrics[viz][stream]*0.85
                v = self.streams_origin_table[stream]
                if v == viz:
                    if self.streams_metrics[viz][stream] < self.streams_metrics[v][stream] and self.streams_metrics[viz][stream] != 0:
                        self.streams_origin_table[stream] = viz
            
    def StreamDeativated(self, viz, streams_id):
        with self.lock:
            for stream_id in streams_id:
                self.streams_metrics[viz][stream_id] = 0
    

    def activateStream(self, viz, stream_id):
        with self.lock:
            is_stream_active = False
            if stream_id in self.available_streams and viz in self.active_streams_table:
                for v in self.vizinhos:
                    if self.active_streams_table[v][stream_id] == 1:
                        is_stream_active = True
                        break
                self.active_streams_table[viz][stream_id] = 1
                print(f"Stream {stream_id} activated for neighbour {viz}.\n")
            else:
                print(f"ERROR: Stream {stream_id} cannot be activated for {viz}.\n")
            return is_stream_active

    def get_streams(self):
        with self.lock:
            """Retorna lista de ids streams"""
            return list(self.available_streams)

    def deactivateStream(self, viz, stream_id):
        with self.lock:
            is_stream_active = False
            if stream_id in self.available_streams and viz in self.active_streams_table:
                self.active_streams_table[viz][stream_id] = 0
                for v in self.vizinhos:
                    if self.active_streams_table[v][stream_id] == 1:
                        is_stream_active = True
                        break
                print(f"Stream {stream_id} deactivated for neighbour {viz}.\n")
            else:
                print(f"ERROR: Stream {stream_id} cannot be deactivated for {viz}.\n")
            return is_stream_active

    def printActiveStreamsTable(self):
        print("         " + " ".join(f"{s:>4}" for s in self.available_streams))
        for v in self.vizinhos:
            valores = " ".join(f"{self.active_streams_table[v][s]:>4}" for s in self.available_streams)
            print(f"{v:>4} {valores}")

    def getStreamSource(self, stream_id):
        stream_source = None
        if stream_id in self.streams_origin_table:
            stream_source = self.streams_origin_table[stream_id]
            return stream_source
        else:
            print(f"ERROR: Stream {stream_id} has no known source.\n")
            return None

    # =============================================================
    #           FUNÇÕES DE OVERLAY / VIZINHOS (arquivo 2)
    # =============================================================

    def inicializaVizinho(self, viz):
        with self.lock:
            if viz in self.vizinhos:
                self.vizinhos_inicializados[viz] = 1
                print(f"Neighbour {viz} initialized.\n")
            else:
                print(f"Neighbour {viz} could not be initialized.\n")

    def removeVizinho(self, viz):
        with self.lock:
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

        print(list(db.get_vizinhos()))
        print(db.get_my_ip(list(db.get_vizinhos())[0]))

        # testes antigos
        db.printActiveStreamsTable()
        db.addStream("s1", "10.0.0.10")
        db.addStream("s2", "10.0.1.2")
        db.addStream("s3", "10.0.1.2")

        db.printActiveStreamsTable()

        db.activateStream("10.0.1.2", "s1")
        db.activateStream("10.0.2.2", "s2")
        db.activateStream("10.0.2.2", "s3")

        db.printActiveStreamsTable()
    else:
        db = DataBase(sys.argv[1])
