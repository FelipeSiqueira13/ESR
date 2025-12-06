import json
import sys
import threading
import socket
import datetime as dt

class ServerDataBase():

    def __init__(self, name):

        with open('server_config.json', 'r') as file:
            server_info = json.load(file)
        
        self.server_streams = server_info.get(name, {}).get("streams", {})
        # server_vizinhos: mapeia cada vizinho conhecido para o estado/métricas que mantemos dele
        # Ex.: {"10.0.0.10": 1, "10.0.1.2": 0}
        self.server_vizinhos = {}
        # stream_vizinhos: mapeia cada stream ativa para a lista de vizinhos que a estão recebendo
        # Ex.: {"stream1": ["10.0.0.10", "10.0.1.2"], "stream2": ["10.0.3.1"]}
        self.stream_vizinhos = {}
        self.lock = threading.Lock()
        self.pending_metric_requests = {}
        self.stream_metrics = {}

        with open('config.json', 'r') as file:
            ip_config = json.load(file)
            data = ip_config.get(name, {})

        self.my_ip = list(data.keys())[0] if data else None

        for ip in data.keys():
            self.server_vizinhos[ip] = 0  # Inicializa estado/métricas do vizinho

    def initiate_stream(self,src,stream_id):
        with self.lock:
            """Adiciona o stream_id ao dicionario de streams_vizinhos"""
            if stream_id not in self.stream_vizinhos:
                self.stream_vizinhos[stream_id] = []
            if src not in self.stream_vizinhos[stream_id]:
                self.stream_vizinhos[stream_id].append(src)
    
    def end_stream(self,src,stream_id):
        with self.lock:
            """Remove o stream_id do dicionario de streams_vizinhos"""
            if stream_id in self.stream_vizinhos:
                if src in self.stream_vizinhos[stream_id]:
                    self.stream_vizinhos[stream_id].remove(src)
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

    def register_metric_request(self, request_id, vizinho, streams, start_time):
        with self.lock:
            self.pending_metric_requests[request_id] = {
                "neighbor": vizinho,
                "streams": list(streams),
                "start_time": start_time
            }

    def record_metric(self, vizinho, streams, delay_ms, request_id=None):
        with self.lock:
            print(f"[SERVER][METRIC_STORE] vizinho={vizinho} streams={streams} delay_ms={delay_ms} request_id={request_id}")
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
        sdb = ServerDataBase("S1")   # alterar depois no trabalho final
        print(sdb.initiate_stream("10.0.0.1","stream1"))
        print(sdb.stream_vizinhos)
        print(sdb.end_stream("10.0.0.1","stream1"))
        print(sdb.stream_vizinhos)

        print(sdb.get_streams())
    else:
        sdb = ServerDataBase(sys.argv[1])
