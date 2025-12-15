import threading
import socket
import sys
import struct
import datetime as dt
import uuid
from server_database import ServerDataBase
from msg import Message
from VideoStream import VideoStream
import time
import json
from SimplePacket import SimplePacket
import hashlib

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334
METRIC_INTERVAL_SECONDS = 3
HEARTBEAT_INTERVAL = 1
HEARTBEAT_TIMEOUT = 5
FPS = 30

def _send_buffer(sock: socket.socket, payload: bytes):
    view = memoryview(payload)
    total_sent = 0
    while total_sent < len(view):
        sent = sock.send(view[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def stream_request_handler(msg, database: ServerDataBase):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.initiate_stream(src, stream_id)
    print(f"SENDING STREAM src={src} stream_id={stream_id}")

def stream_stop_handler(msg, database: ServerDataBase):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.end_stream(src, stream_id)
    print(f"Stream {stream_id} stopped for {src}")

def listener(sdb:ServerDataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind((sdb.my_ip, RECEIVER_PORT))
    sckt.listen(10)

    while True:
        try:
            conn, addr = sckt.accept()
            
            def handle_connection():
                try:
                    buffer = b''
                    while True:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        buffer += chunk
                        while b'\n' in buffer:
                            dados, buffer = buffer.split(b'\n', 1)
                            if dados:
                                msg = Message.deserialize(dados)
                                typeOfMsg = msg.getType()
                                print(f"from={addr[0]} type={typeOfMsg} data={msg.getData()}")

                                # qualquer mensagem toca o vizinho
                                sdb.touch_neighbor(addr[0])

                                if typeOfMsg == Message.STREAM_REQUEST:
                                    threading.Thread(target=stream_request_handler, args=(msg, sdb), daemon=True).start()
                                elif typeOfMsg == Message.STREAM_STOP:
                                    threading.Thread(target=stream_stop_handler, args=(msg, sdb), daemon=True).start()
                except Exception as e:
                    print(f"Error handling connection: {e}")
                finally:
                    conn.close()
            
            threading.Thread(target=handle_connection, daemon=True).start()
        except Exception as e:
            print(f"Error in listener: {e}")
    sckt.close()

def _stream_id_to_int(stream_id: str) -> int:
    """Mapeia o id textual da stream para um inteiro de 32 bits (consistente)."""
    return int.from_bytes(hashlib.sha256(stream_id.encode('utf-8')).digest()[:4], 'big', signed=False)

def sender(sdb:ServerDataBase):
    print("Sender thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    streams_active = {}
    
    # Controle de tempo para cada stream: stream_id -> next_frame_time
    stream_next_time = {}
    
    while True:
        now = time.time()
        loop_start = now
        
        try:
            with sdb.lock:
                stream_viz_copy = dict(sdb.stream_vizinhos)
            
            for stream_id, viz in stream_viz_copy.items():
                if stream_id not in streams_active:
                    path = sdb.server_streams.get(stream_id)
                    if path:
                        try:
                            streams_active[stream_id] = VideoStream(path)
                            stream_next_time[stream_id] = now
                        except Exception as e:
                            print(f"Error loading VideoStream for {stream_id}: {e}")
                            continue

                vs = streams_active.get(stream_id)
                if not vs:
                    continue

                target_time = stream_next_time.get(stream_id, now)
                
                if target_time > now:
                    continue
                
                if now - target_time > 0.5:
                    target_time = now
                
                stream_next_time[stream_id] = target_time + (1.0 / FPS)

                frame = vs.nextFrame()
                if frame:
                    stream_key = stream_id 
                    stream_num = _stream_id_to_int(stream_key)
                    
                    packed_frames = struct.pack("!B", 1) + struct.pack("!I", len(frame)) + frame

                    payload = stream_key.encode('utf-8') + b'\0' + packed_frames
                    
                    packet_bytes = SimplePacket.encode(
                        stream_id=stream_num,
                        frame_num=vs.frameNbr(),
                        timestamp=time.time(),
                        frame_data=payload
                    )

                    for vizinho in viz:
                        try:
                            sckt.sendto(packet_bytes, (vizinho, SENDER_PORT))
                        except Exception as e:
                            print(f"Error sending frame {vs.frameNbr()} for {stream_key} to {vizinho}: {e}")
            
            time.sleep(1/30)

        except Exception as e:
            print(f"Error in sender: {e}")
    sckt.close()

def cntrl(sdb:ServerDataBase):
    print("Control thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind((sdb.my_ip, ROUTERS_RECEIVER_PORT))
    sckt.listen(10)

    while True:
        try:
            conn, addr = sckt.accept()
            
            def handle_router_connection(conn, addr):
                try:
                    buffer = b''
                    while True:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        buffer += chunk
                        while b'\n' in buffer:
                            dados, buffer = buffer.split(b'\n', 1)
                            if dados:
                                msg = Message.deserialize(dados)
                                msgr_ip = msg.getSrc()
                                typeOfMsg = msg.getType() 
                                # marca último contacto
                                sdb.touch_neighbor(msgr_ip)
                                if typeOfMsg == Message.ADD_NEIGHBOUR:
                                    print(f"ADD_NEIGHBOUR from {msgr_ip}")
                                    sdb.inicializaVizinho(msgr_ip)
                                elif typeOfMsg == Message.RESP_NEIGHBOUR:
                                    sdb.inicializaVizinho(msgr_ip)
                                elif typeOfMsg == Message.VIDEO_METRIC_RESPONSE:
                                    resp = Message(Message.VIDEO_METRIC_RESPONSE, sdb.get_my_ip(msgr_ip), msg.getData())
                                    send_control_message(msgr_ip, resp)
                                elif typeOfMsg == Message.VIDEO_METRIC_UPDATE:
                                    try:
                                        payload = json.loads(msg.getData() or "{}")
                                        print(f"VIDEO_METRIC_UPDATE from {msgr_ip}: {payload}")
                                    except json.JSONDecodeError:
                                        print("Invalid VIDEO_METRIC_UPDATE payload received.")
                                        continue
                                    streams = payload.get("streams", [])
                                    delay_ms = payload.get("delay_ms")
                                    request_id = payload.get("request_id")
                                    if streams and delay_ms is not None:
                                        sdb.record_metric(msgr_ip, streams, delay_ms, request_id)
                except Exception as e:
                    print(f"Error in router connection: {e}")
                finally:
                    conn.close()
            
            threading.Thread(target=handle_router_connection, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print(f"Error in control listener: {e}")
    sckt.close()


def send_control_message(host, message: Message):
    try:
        with socket.create_connection((host, ROUTERS_RECEIVER_PORT), timeout=5) as ctrl:
            _send_buffer(ctrl, message.serialize() + b'\n')
    except Exception as e:
        print(f"Failed to send control message to {host}: {e}")


def _send_control_quiet(host, message: Message):
    """Versão silenciosa usada apenas pelos heartbeats para não poluir o terminal."""
    try:
        with socket.create_connection((host, ROUTERS_RECEIVER_PORT), timeout=5) as ctrl:
            _send_buffer(ctrl, message.serialize() + b'\n')
    except Exception:
        pass


def heartbeat_sender(sdb: ServerDataBase):
    """Envia ADD_NEIGHBOUR periódico aos vizinhos conhecidos."""
    while True:
        try:
            with sdb.lock:
                neighbors = list(sdb.server_vizinhos.keys())
            for viz in neighbors:
                msg = Message(Message.ADD_NEIGHBOUR, sdb.get_my_ip(viz), "")
                _send_control_quiet(viz, msg)
        except Exception as e:
            pass
        finally:
            time.sleep(HEARTBEAT_INTERVAL)


def heartbeat_check(sdb: ServerDataBase):
    """Marca vizinho down após timeout; remove de stream_vizinhos/server_vizinhos."""
    while True:
        try:
            with sdb.lock:
                neighbors = list(sdb.server_vizinhos.keys())
            for viz in neighbors:
                if not sdb.is_neighbor_alive(viz, HEARTBEAT_TIMEOUT):
                    sdb.mark_neighbor_down(viz)
        except Exception as e:
            pass
        finally:
            time.sleep(HEARTBEAT_INTERVAL)

def measure_rtt(host: str, port: int = ROUTERS_RECEIVER_PORT, timeout: float = 1.0) -> float:
    """Mede RTT aproximado até host:port em ms; retorna valor alto em caso de erro."""
    try:
        t0 = time.time()
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return (time.time() - t0) * 1000.0
    except Exception:
        return 1_000_000.0

def metric_updater(sdb:ServerDataBase):
    print("Metric updater thread started")
    while True:
        try:
            awake_neighbors = sdb.get_awake_neighbors()
            
            # NOVO: Filtra apenas vizinhos vivos
            alive_neighbors = [
                viz for viz in awake_neighbors 
                if sdb.is_neighbor_alive(viz, HEARTBEAT_TIMEOUT)
            ]
            
            if not alive_neighbors:
                print("[SERVER][METRICS] No alive neighbors to send metrics")
                time.sleep(METRIC_INTERVAL_SECONDS)
                continue
            
            for viz in alive_neighbors:
                streams = sdb.get_streams()
                request_id = f"req-{uuid.uuid4().hex[:10]}"
                sdb.register_metric_request(request_id, viz, streams)
                
                rtt = measure_rtt(viz)
                
                print(f"METRIC_UPDATE to={viz} streams={streams} request_id={request_id} rtt={rtt:.2f}ms")
                msg = Message(Message.VIDEO_METRIC_REQUEST, sdb.get_my_ip(viz))
                msg.metrics_encode(streams, request_id=request_id, accumulated_delay_ms=rtt)
                send_control_message(viz, msg)
        except Exception as e:
            print("Error in metric updater: ", e)
        finally:
            time.sleep(METRIC_INTERVAL_SECONDS)

def main():
    if len(sys.argv) < 2:
        print("Usage: python server.py <server_name>")
        sys.exit(1)
        
    sdb = ServerDataBase(sys.argv[1])
    print(f"Server {sys.argv[1]} started.\n")
    
    thread_listen = threading.Thread(target=listener, args=(sdb,))
    thread_sender = threading.Thread(target=sender, args=(sdb,))
    thread_cntrl = threading.Thread(target=cntrl, args=(sdb,))
    thread_metrics = threading.Thread(target=metric_updater, args=(sdb,))
    thread_hb = threading.Thread(target=heartbeat_sender, args=(sdb,))
    thread_hbchk = threading.Thread(target=heartbeat_check, args=(sdb,))

    all_threads = [thread_listen, thread_sender, thread_cntrl, thread_metrics, thread_hb, thread_hbchk]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

if __name__ == '__main__':
    main()
