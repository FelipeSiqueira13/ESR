from asyncio import streams
import json
import sys
import threading
import math
import datetime as dt
from pathlib import Path

CURRENT_TOPOLOGY = "topology2"

class DataBase():

    def __init__(self, name):

        # ----------- CARREGA CONFIG E VIZINHOS DO ARQUIVO 1 -----------
        config_file = Path("topologies") / CURRENT_TOPOLOGY / f"{name}.json"

        try:
            with open(config_file, 'r') as file:
                ip_config = json.load(file)
        except FileNotFoundError:
            print("Error: File Not Found")
            sys.exit(1)

        # agora os valores podem ser listas; normaliza para dict ip -> [viz1, viz2, ...]
        raw = ip_config.get(name, {})
        self.ip_to_viz = {ip: (v if isinstance(v, list) else [v]) for ip, v in raw.items()}
        
        # Define my_ip para binding
        my_ips = list(raw.keys())
        if len(my_ips) == 1:
            self.my_ip = my_ips[0]
        else:
            # Se tiver multiplas interfaces (Router) ou nenhuma, bind em todas (0.0.0.0)
            # Isso garante funcionamento no CORE para routers e evita erros de logica
            self.my_ip = "0.0.0.0"

        self.lock = threading.RLock()
        self.neighbor_last_seen = {}  # vizinho -> datetime
        self.best_parent = {}         # stream_id -> ip vizinho
        self.best_cost = {}           # stream_id -> métrica
        self.seen_msgs = set()
        self.hysteresis_factor = 0.8  # o valor é alterado por onode.py
        self.downstream = {}          # stream_id -> set(vizinhos downstream ativos)

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

        self.pending_metric_requests = {}

    def store_metric_request(self, request_id, data):
        with self.lock:
            self.pending_metric_requests[request_id] = data

    def get_metric_request(self, request_id):
        with self.lock:
            return self.pending_metric_requests.get(request_id)

    def remove_metric_request(self, request_id):
        with self.lock:
            self.pending_metric_requests.pop(request_id, None)

    def get_streams(self):
        with self.lock:
            return list(self.available_streams)

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
                # Aplica EMA: 90% histórico + 10% novo valor
                old_metric = self.streams_metrics[viz].get(stream, 0)
                self.streams_metrics[viz][stream] = metric * 0.10 + old_metric * 0.90
                
                # Atualiza origem se métrica for melhor
                current_origin = self.streams_origin_table.get(stream)
                if current_origin and viz != current_origin:
                    if (self.streams_metrics[viz][stream] < self.streams_metrics[current_origin][stream] 
                        and self.streams_metrics[viz][stream] > 0):
                        self.streams_origin_table[stream] = viz
                        
                print(f"[ONODE][METRIC_APPLY] viz={viz} stream={stream} metric={self.streams_metrics[viz][stream]:.2f}")
    
    def StreamDeativated(self, viz, streams_id):
        with self.lock:
            for stream_id in streams_id:
                self.streams_metrics[viz][stream_id] = 0
    

    def activateStream(self, viz, stream_id):
        with self.lock:
            if stream_id in self.available_streams and viz in self.active_streams_table:
                # Use .get() to avoid KeyError if stream_id is missing for some neighbor
                already_active = any(
                    self.active_streams_table[v].get(stream_id, 0) == 1 for v in self.vizinhos
                )
                self.active_streams_table[viz][stream_id] = 1
                print(f"Stream {stream_id} activated for neighbour {viz}.\n")
                # precisa pedir upstream se antes não havia nenhum ativo
                need_upstream = not already_active
            else:
                print(f"ERROR: Stream {stream_id} cannot be activated for {viz}.\n")
                need_upstream = False
            return need_upstream

    def deactivateStream(self, viz, stream_id):
        with self.lock:
            if stream_id in self.available_streams and viz in self.active_streams_table:
                self.active_streams_table[viz][stream_id] = 0
                
                # Debug: Print state of all neighbors for this stream
                print(f"[DB][DEACTIVATE] Checking active status for stream {stream_id}:")
                for v in self.vizinhos:
                    status = self.active_streams_table[v].get(stream_id, "N/A")
                    print(f"  - {v}: {status}")

                still_active = any(
                    self.active_streams_table[v].get(stream_id, 0) == 1 for v in self.vizinhos
                )
                print(f"Stream {stream_id} deactivated for neighbour {viz}. Still active? {still_active}\n")
            else:
                print(f"ERROR: Stream {stream_id} cannot be deactivated for {viz}. (Stream or Viz not found)\n")
                still_active = False
            # retorna se ainda existe alguém ativo (True => não propaga STOP)
            return still_active

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

    def touch_neighbor(self, viz):
        with self.lock:
            self.neighbor_last_seen[viz] = dt.datetime.now()

    def is_neighbor_alive(self, viz, timeout_s: float) -> bool:
        with self.lock:
            ts = self.neighbor_last_seen.get(viz)
        if not ts:
            return False
        return (dt.datetime.now() - ts).total_seconds() < timeout_s

    # ---------------- Best parent/cost ----------------
    def update_announce(self, stream_id: str, cost: float, parent_ip: str) -> bool:
        with self.lock:
            prev = self.best_cost.get(stream_id, math.inf)
            curr_parent = self.best_parent.get(stream_id)

            # Se for do mesmo pai, atualiza sempre (para refletir piora ou melhora)
            if parent_ip == curr_parent:
                self.best_cost[stream_id] = cost
                return True

            if cost < prev * self.hysteresis_factor:
                self.best_cost[stream_id] = cost
                self.best_parent[stream_id] = parent_ip
                return True
            return False

    def get_best_parent(self, stream_id: str):
        with self.lock:
            return self.best_parent.get(stream_id)

    def get_best_cost(self, stream_id: str):
        with self.lock:
            return self.best_cost.get(stream_id, math.inf)

    # ---------------- Downstream ativos ----------------
    def set_downstream_active(self, viz: str, stream_id: str, active: bool):
        with self.lock:
            ds = self.downstream.setdefault(stream_id, set())
            if active:
                ds.add(viz)
            else:
                ds.discard(viz)

    def get_downstream(self, stream_id: str):
        with self.lock:
            return set(self.downstream.get(stream_id, set()))

    def has_downstream(self, stream_id: str) -> bool:
        with self.lock:
            return bool(self.downstream.get(stream_id))


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
