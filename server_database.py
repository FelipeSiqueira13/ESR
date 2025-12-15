import json
import sys
import threading
import socket
import datetime as dt
from pathlib import Path 

CURRENT_TOPOLOGY = "topology2"
class ServerDataBase():
    def __init__(self, name):
        self.name = name

        config_file = Path("topologies") / CURRENT_TOPOLOGY / f"{name}.json"

        try:
            with open(config_file, 'r') as file:
                server_info = json.load(file)
        except FileNotFoundError:
            print("Error: File Not Found!")
            sys.exit(1)
        
        server_data = server_info.get(name, {})

        self.server_streams = server_data.get("streams", {})

        ip_data = {k: v for k, v in server_data.items() if k != "streams"}

        # server_vizinhos: mapeia cada vizinho conhecido para o estado/métricas que mantemos dele
        # Ex.: {"10.0.0.10": 1, "10.0.1.2": 0}
        self.server_vizinhos = {}
        # stream_vizinhos: mapeia cada stream ativa para a lista de vizinhos que a estão recebendo
        # Ex.: {"stream1": ["10.0.0.10", "10.0.1.2"], "stream2": ["10.0.3.1"]}
        self.stream_vizinhos = {}
        self.lock = threading.Lock()
        self.neighbor_last_seen = {}  # vizinho -> datetime
        self.pending_metric_requests = {}
        self.stream_metrics = {}

        self.my_ip = None
        if ip_data:
            first_ip = list(ip_data.keys())[0]
            self.my_ip = first_ip

        # Inicializa estado para cada interface local e cada vizinho configurado
        for ip, neighs in ip_data.items():
            self.server_vizinhos[ip] = 0  # minhas interfaces desligadas
            self.neighbor_last_seen[ip] = None
            for viz in (neighs if isinstance(neighs, list) else [neighs]):
                self.server_vizinhos[viz] = 0
                self.neighbor_last_seen[viz] = None

    def touch_neighbor(self, viz):
        with self.lock:
            self.neighbor_last_seen[viz] = dt.datetime.now()

    def is_neighbor_alive(self, viz, timeout_s: float) -> bool:
        with self.lock:
            ts = self.neighbor_last_seen.get(viz)
        if not ts:
            return False
        return (dt.datetime.now() - ts).total_seconds() < timeout_s

    def mark_neighbor_down(self, viz):
        with self.lock:
            if viz in self.server_vizinhos:
                self.server_vizinhos[viz] = 0
            # remove viz das listas de streams
            to_clean = []
            for stream, lst in self.stream_vizinhos.items():
                if viz in lst:
                    lst.remove(viz)
                if not lst:
                    to_clean.append(stream)
            for stream in to_clean:
                self.stream_vizinhos.pop(stream, None)
            # limpa métricas e pending
            self.stream_metrics.pop(viz, None)

    def initiate_stream(self,src,stream_id):
        with self.lock:
            """Adiciona o stream_id ao dicionario de streams_vizinhos"""
            print(f"initiate_stream called for src={src} stream={stream_id}")
            if stream_id not in self.stream_vizinhos:
                self.stream_vizinhos[stream_id] = []
            if src not in self.stream_vizinhos[stream_id]:
                self.stream_vizinhos[stream_id].append(src)
                print(f"Added {src} to stream {stream_id}. Current list: {self.stream_vizinhos[stream_id]}")
            else:
                print(f"{src} already in stream {stream_id}")
    
    def end_stream(self,src,stream_id):
        with self.lock:
            """Remove o stream_id do dicionario de streams_vizinhos"""
            print(f"end_stream called for src={src} stream={stream_id}")
            if stream_id in self.stream_vizinhos:
                if src in self.stream_vizinhos[stream_id]:
                    self.stream_vizinhos[stream_id].remove(src)
                    print(f"Removed {src} from stream {stream_id}")
                    if not self.stream_vizinhos[stream_id]:
                        del self.stream_vizinhos[stream_id]
    
    def get_streams_vizinhos(self):
        with self.lock:
            """Retorna uma cópia do dicionario de streams_vizinhos"""
            return {stream: list(vizinhos) for stream, vizinhos in self.stream_vizinhos.items()}

    def get_my_ip(self, vizinho_ip):
        return self.my_ip

    def get_streams(self):
        with self.lock:
            """Retorna lista de ids streams"""
            return list(self.server_streams.keys())

    def inicializaVizinho(self, ip):
        with self.lock:
            if ip not in self.server_vizinhos:
                self.server_vizinhos[ip] = 0
            self.server_vizinhos[ip] = 1

    def get_streams_for_neighbor(self, vizinho):
        with self.lock:
            return [stream for stream, vizinhos in self.stream_vizinhos.items() if vizinho in vizinhos]

    def get_awake_neighbors(self):
        with self.lock:
            return [viz for viz, status in self.server_vizinhos.items() if status == 1]

    def register_metric_request(self, request_id, vizinho, streams):
        with self.lock:
            self.pending_metric_requests[request_id] = {
                "neighbor": vizinho,
                "streams": list(streams)
            }

    def record_metric(self, vizinho, streams, delay_ms, request_id=None):
        with self.lock:
            print(f"salvar metricas vizinho={vizinho} streams={streams} delay_ms={delay_ms} request_id={request_id}")
            metrics = self.stream_metrics.setdefault(vizinho, {})
            timestamp = dt.datetime.now().isoformat()
            for stream in streams:
                metrics[stream] = {
                    "delay_ms": delay_ms,
                    "updated_at": timestamp,
                    "request_id": request_id
                }
            if request_id:
                self.pending_metric_requests.pop(request_id, None)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("error: missing server name argument")
    else:
        sdb = ServerDataBase(sys.argv[1])
