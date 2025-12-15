import socket
import sys
import threading
import queue
import datetime as dt
import json, uuid
import math
import time
import struct
from typing import Optional
from database import DataBase
from msg import Message
from SimplePacket import SimplePacket

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334

MAX_RETRIES = 3

send_queue = queue.Queue()

FRAME_BUFFER_SIZE = 100
_frame_buffer = {}
_frame_lock = threading.Lock()

TTL_DEFAULT = 8
ANNOUNCE_TYPE = Message.VIDEO_METRIC_REQUEST  # reutilizamos como ANNOUNCE

HEARTBEAT_INTERVAL = 1      # segundos
HEARTBEAT_TIMEOUT  = 3      # segundos (reduzido para detecção mais rápida de falhas)
HYST_FACTOR = 0.80           

HOP_PENALTY_PERCENT = 0.15
CACHE_BONUS_PERCENT = 0.35   


def log_ev(tag: str, **fields):
    parts = [f"[{tag}]"] + [f"{k}={v}" for k, v in fields.items()]
    print(" ".join(parts))

def _send_buffer(sock: socket.socket, payload: bytes):
    view = memoryview(payload)
    total_sent = 0
    while total_sent < len(view):
        sent = sock.send(view[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def send_message(msg:Message, host:str, port:int):
    send_queue.put((msg, host, port))

def _upstream_for(stream_id: str, db: DataBase):
    """Escolhe o vizinho upstream (best_parent); fallback para origem conhecida."""
    parent = db.get_best_parent(stream_id)
    if parent:
        return parent
    return db.getStreamSource(stream_id)

def _apply_delay_adjustments(current_delay_ms: float, stream_id: str, db: DataBase) -> float:
    
    penalized_delay = current_delay_ms * (1 + HOP_PENALTY_PERCENT)
    
    # Verifica se ESTA stream específica está ativa localmente
    has_cache = _is_stream_active_locally(stream_id, db)
    
    if has_cache:
        adjusted_delay = penalized_delay * (1 - CACHE_BONUS_PERCENT)
        return adjusted_delay
    
    return penalized_delay


def _check_and_switch_parent(stream, current_upstream, db):
    new_upstream = _upstream_for(stream, db)
    if current_upstream and new_upstream and current_upstream != new_upstream:
        if db.has_downstream(stream):
            log_ev("ROUTE_SWITCH", stream=stream, old=current_upstream, new=new_upstream)
            
            # 1. Request from new parent
            req = Message(Message.STREAM_REQUEST, db.get_my_ip(new_upstream), stream)
            send_message(req, new_upstream, RECEIVER_PORT)
            
            # 2. Stop from old parent
            stop = Message(Message.STREAM_STOP, db.get_my_ip(current_upstream), stream)
            send_message(stop, current_upstream, RECEIVER_PORT)

def stream_pls_handler(msg: Message, db: DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    need_upstream = db.activateStream(ip_viz, stream_id)
    db.set_downstream_active(ip_viz, stream_id, True)
    print(f"Processed STREAM_PLS for stream {stream_id} from {msg.getSrc()}.\n")
    if need_upstream:
        parent = _upstream_for(stream_id, db)
        if parent:
            req = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
            send_message(req, parent, RECEIVER_PORT)
            print(f"STREAM REQUEST stream={stream_id} -> {parent}")
        else:
            print(f"STREAM REQUEST NO PARENT stream={stream_id}")

def stream_no_handler(msg: Message, db: DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    # Atualiza tabelas
    db.deactivateStream(ip_viz, stream_id)
    db.set_downstream_active(ip_viz, stream_id, False)
    
    # Verifica se ainda há algum vizinho downstream interessado
    has_downstream = db.has_downstream(stream_id)
    print(f"Processed VIDEO_NO for stream {stream_id} from {msg.getSrc()}. Has downstream? {has_downstream}\n")
    
    if not has_downstream:
        parent = _upstream_for(stream_id, db)
        if parent:
            stop = Message(Message.STREAM_STOP, db.get_my_ip(parent), stream_id)
            send_message(stop, parent, RECEIVER_PORT)
            print(f"STREAM STOP stream={stream_id} -> {parent}")
        else:
            print(f"STREAM STOP NO PARENT stream={stream_id}")

def stream_request_handler(stream_id, db: DataBase):
    """Mantido para compatibilidade, agora usa best_parent."""
    parent = _upstream_for(stream_id, db)
    if parent:
        msg = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
        send_message(msg, parent, RECEIVER_PORT)
        print(f"STREAM REQUEST stream={stream_id} -> {parent}")

def stream_stop_handler(stream_id, db: DataBase):
    parent = _upstream_for(stream_id, db)
    if parent:
        msg = Message(Message.STREAM_STOP, db.get_my_ip(parent), stream_id)
        send_message(msg, parent, RECEIVER_PORT)
        print(f"STREAM STOP stream={stream_id} -> {parent}")

def streams_available_handler(msg, db:DataBase):
    streams = db.get_streams() # depois tirar o sbd 
    data = ",".join(streams)
    if data == "":
        data = "No streams available"

    source = msg.getSrc()
  
    response = Message(Message.RESP_STREAMS_AVAILABLE, db.get_my_ip(source), data)
    send_message(response, source, RECEIVER_PORT)


# =============================================================
#                      ROUTER THREADS
# =============================================================

def wake_router_handler(vizinhos, db:DataBase):
    viz_list = list(vizinhos) if not isinstance(vizinhos, list) else vizinhos
    for viz in viz_list:
        src_ip = db.get_my_ip(viz)
        msg = Message(Message.ADD_NEIGHBOUR, src_ip, "")
        send_message(msg, viz, ROUTERS_RECEIVER_PORT)

def update_metrics(streams_id:list, metric, db:DataBase, viz):
    db.AtualizaMetricas(viz, streams_id, metric)

def _is_stream_active_locally(stream_id: str, db: DataBase) -> bool:
    if db.has_downstream(stream_id):
        return True

    with db.lock:
        for viz in db.vizinhos:
            if db.active_streams_table.get(viz, {}).get(stream_id) == 1:
                return True

    with _stream_last_seen_lock:
        last_seen = _stream_last_seen.get(stream_id, 0)
        if time.time() - last_seen < 2.0:
            return True
    
    return False

def metric_request_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"Error decoding metrics: {e}")
        import traceback
        traceback.print_exc()
        return

    request_id = payload.get("request_id")
    streams = payload.get("streams", [])
    accumulated_delays = payload.get("accumulated_delays", {})
    
    if not isinstance(accumulated_delays, dict):
        if isinstance(accumulated_delays, (int, float)):
            accumulated_delays = {s: float(accumulated_delays) for s in streams}
        else:
            accumulated_delays = {s: 0.0 for s in streams}
    
    if not request_id:
        return

    sender = msg.getSrc()
    
    stream_costs = {}
    
    for stream in streams:
        incoming_delay = float(accumulated_delays.get(stream, 0))
        total_delay = _apply_delay_adjustments(incoming_delay, stream, db)
        stream_costs[stream] = total_delay
        
        log_ev("METRIC_REQ_RECV", 
               req=request_id,
               stream=stream,
               delay_before=incoming_delay,
               delay_after=total_delay,
               local_cache=_is_stream_active_locally(stream, db),
               from_=sender)
    
    db.store_metric_request(request_id, {
        "streams": streams,
        "src": sender
    })

    should_propagate = False
    streams_to_propagate = []
    
    for stream in streams:
        if stream not in db.available_streams:
            db.addStream(stream, sender)
        
        current_upstream = _upstream_for(stream, db)
        cost = stream_costs[stream]
        
        if db.update_announce(stream, cost, sender):
            should_propagate = True
            streams_to_propagate.append(stream)
            _check_and_switch_parent(stream, current_upstream, db)

    if not should_propagate:
        log_ev("METRIC_REQ_DROP", req=request_id, msg="Worse paths, not propagating")
        return

    neighbors = db.get_vizinhos()
    
    # NOVO: Filtra apenas vizinhos vivos (excluindo sender)
    targets = [
        n for n in neighbors 
        if n != sender and db.is_neighbor_alive(n, HEARTBEAT_TIMEOUT)
    ]
    
    if streams_to_propagate:
        for neighbor in targets:
            rtt = measure_rtt(neighbor)
            
            new_accumulated_delays = {}
            for stream in streams_to_propagate:
                new_accumulated_delays[stream] = stream_costs[stream] + rtt
            
            fwd_msg = Message(Message.VIDEO_METRIC_REQUEST, db.get_my_ip(neighbor))
            fwd_msg.metrics_encode_per_stream(
                streams_to_propagate,
                request_id=request_id,
                accumulated_delays=new_accumulated_delays
            )
            send_message(fwd_msg, neighbor, ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_REQ_FWD", req=request_id, to=neighbor, streams=streams_to_propagate, rtt=rtt)
    else:
        log_ev("METRIC_REQ_LEAF", req=request_id, msg="No neighbors or no improved streams")


def metric_response_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"Failed to decode: {e}")
        return
    
    if not isinstance(payload, dict):
        print(f"Invalid payload type: {type(payload)}")
        return
    
    request_id = payload.get("request_id")
    accumulated_delays = payload.get("accumulated_delays", {})
    streams = payload.get("streams", [])
    
    if not isinstance(accumulated_delays, dict):
        if isinstance(accumulated_delays, (int, float)):
            accumulated_delays = {s: float(accumulated_delays) for s in streams}
        else:
            accumulated_delays = {s: 0.0 for s in streams}
    
    if not request_id or not streams:
        print("Missing request_id or streams")
        return
    
    local_rtt_ms = measure_rtt(msg.getSrc())
    
    stream_costs = {}
    
    for stream in streams:
        incoming_delay = float(accumulated_delays.get(stream, 0))
        delay_before_adjustments = incoming_delay + local_rtt_ms
        
        total_delay_ms = _apply_delay_adjustments(delay_before_adjustments, stream, db)
        stream_costs[stream] = total_delay_ms
        
        log_ev("METRIC_RESP_RECV",
               req=request_id,
               stream=stream,
               cost=total_delay_ms,
               parent=msg.getSrc(),
               incoming_ms=incoming_delay,
               rtt_ms=local_rtt_ms,
               has_cache=_is_stream_active_locally(stream, db))

    for stream in streams:
        db.AtualizaMetricas(msg.getSrc(), [stream], stream_costs[stream])
        current_upstream = _upstream_for(stream, db)
        if db.update_announce(stream, stream_costs[stream], msg.getSrc()):
            _check_and_switch_parent(stream, current_upstream, db)

    stored = db.get_metric_request(request_id)
    
    if stored:
        db.remove_metric_request(request_id)
        log_ev("METRIC_RESP_ORIGIN", req=request_id, streams=streams, 
               costs={s: stream_costs[s] for s in streams})
        return

    # Propaga downstream
    downstream_neighbors = set()
    with db.lock:
        for stream in streams:
            for viz, active_streams in db.active_streams_table.items():
                if active_streams.get(stream) == 1 and viz != msg.getSrc():
                    downstream_neighbors.add(viz)

    alive_downstream = [
        viz for viz in downstream_neighbors
        if db.is_neighbor_alive(viz, HEARTBEAT_TIMEOUT)
    ]

    if alive_downstream:
        for downstream in alive_downstream:
            fwd_msg = Message(Message.VIDEO_METRIC_RESPONSE, db.get_my_ip(downstream))
            fwd_msg.metrics_encode_per_stream(
                streams,
                request_id=request_id,
                accumulated_delays=stream_costs
            )
            send_message(fwd_msg, downstream, ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_RESP_FWD", req=request_id, to=downstream, costs=stream_costs)
    else:
        log_ev("METRIC_RESP_NO_ALIVE_DOWN", req=request_id, msg="No alive downstream neighbors")


def metric_update_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"Failed to decode: {e}")
        return
    
    if not isinstance(payload, dict):
        print(f"Invalid payload type")
        return
    
    streams = payload.get("streams", [])
    accumulated_delays = payload.get("accumulated_delays", {})
    request_id = payload.get("request_id")
    
    if not isinstance(accumulated_delays, dict):
        if isinstance(accumulated_delays, (int, float)):
            accumulated_delays = {s: float(accumulated_delays) for s in streams}
        else:
            accumulated_delays = {s: 0.0 for s in streams}
    
    if not streams:
        print("Missing streams")
        return
    
    stream_costs = {}
    
    for stream in streams:
        incoming_delay = float(accumulated_delays.get(stream, 0))
        adjusted_delay = _apply_delay_adjustments(incoming_delay, stream, db)
        stream_costs[stream] = adjusted_delay
        
        log_ev("METRIC_UPDATE_RECV",
               req=request_id,
               stream=stream,
               cost=adjusted_delay,
               parent=msg.getSrc(),
               has_cache=_is_stream_active_locally(stream, db))
    
    for stream in streams:
        db.AtualizaMetricas(msg.getSrc(), [stream], stream_costs[stream])
        current_upstream = _upstream_for(stream, db)
        if db.update_announce(stream, stream_costs[stream], msg.getSrc()):
            _check_and_switch_parent(stream, current_upstream, db)

    stored = db.get_metric_request(request_id) if request_id else None
    
    if stored:
        downstream = stored.get("src")
        if downstream and downstream != msg.getSrc() and db.is_neighbor_alive(downstream, HEARTBEAT_TIMEOUT):
            rtt = measure_rtt(downstream)
            
            new_delays = {s: stream_costs[s] + rtt for s in streams}
            
            update_msg = Message(Message.VIDEO_METRIC_UPDATE, db.get_my_ip(downstream))
            update_msg.metrics_encode_per_stream(
                streams,
                request_id=request_id,
                accumulated_delays=new_delays
            )
            send_message(update_msg, downstream, ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_UPDATE_FWD", req=request_id, to=downstream, costs=new_delays)
        elif downstream and downstream != msg.getSrc():
            log_ev("METRIC_UPDATE_DOWN_DEAD", req=request_id, downstream=downstream)
        
        db.remove_metric_request(request_id)
    else:
        with db.lock:
            downstream_neighbors = set()
            for stream in streams:
                for viz, active_streams in db.active_streams_table.items():
                    if active_streams.get(stream) == 1 and viz != msg.getSrc():
                        downstream_neighbors.add(viz)

            alive_downstream = [
                viz for viz in downstream_neighbors
                if db.is_neighbor_alive(viz, HEARTBEAT_TIMEOUT)
            ]
            
            for downstream in alive_downstream:
                rtt = measure_rtt(downstream)
                new_delays = {s: stream_costs[s] + rtt for s in streams}
                
                update_msg = Message(Message.VIDEO_METRIC_UPDATE, db.get_my_ip(downstream))
                update_msg.metrics_encode_per_stream(
                    streams,
                    request_id=request_id or f"fwd-{int(dt.datetime.now().timestamp()*1000)}",
                    accumulated_delays=new_delays
                )
                send_message(update_msg, downstream, ROUTERS_RECEIVER_PORT)
                log_ev("METRIC_UPDATE_BCAST", req=request_id, to=downstream, costs=new_delays)
            
            if not alive_downstream and downstream_neighbors:
                log_ev("METRIC_UPDATE_ALL_DOWN_DEAD", 
                       req=request_id, 
                       dead_count=len(downstream_neighbors))

# =============================================================
#                      PRINCIPAL THREADS
# =============================================================

def listener(db:DataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind((db.my_ip, RECEIVER_PORT))
    sckt.listen(10) #Aceita até 10 conexões em espera

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
                            dados, buffer = buffer.split(b'\n',1)
                            if dados:
                                msg = Message.deserialize(dados)
                                typeOfMsg = msg.getType() 
                                print(f"MSG from={addr[0]} type={typeOfMsg} data={msg.getData()}")
                                
                                if typeOfMsg == Message.STREAMS_AVAILABLE:
                                    streams = db.get_streams()
                                    data = ",".join(streams) if streams else "No streams available"
                                    resp = Message(Message.RESP_STREAMS_AVAILABLE, db.get_my_ip(addr[0]), data)
                                    _send_buffer(conn, resp.serialize() + b'\n')
                                
                                elif typeOfMsg == Message.STREAM_REQUEST:
                                    stream_id = msg.getData()
                                    if stream_id in db.get_streams():
                                        threading.Thread(target=stream_pls_handler, args=(msg, db)).start()
                                        resp = Message(Message.STREAM_REQUEST, db.get_my_ip(addr[0]), "OK")
                                        _send_buffer(conn, resp.serialize() + b'\n')
                                    else:
                                        resp = Message(Message.STREAM_REQUEST, db.get_my_ip(addr[0]), "Stream not found")
                                        _send_buffer(conn, resp.serialize() + b'\n')

                                elif typeOfMsg == Message.STREAM_STOP:
                                    threading.Thread(target=stream_no_handler, args=(msg, db)).start()
                                    resp = Message(Message.STREAM_STOP, db.get_my_ip(addr[0]), "OK")
                                    _send_buffer(conn, resp.serialize() + b'\n')

                                elif typeOfMsg == Message.PING:
                                    threading.Thread(target=ping_handler, args=(msg, db)).start()
                                    resp = Message(Message.PING, db.get_my_ip(addr[0]), "PING_ACK")
                                    _send_buffer(conn, resp.serialize() + b'\n')

                                else:
                                    pass
                except Exception as e:
                    print("Error handling connection: ", e)
                finally:
                    conn.close()
            threading.Thread(target=handle_connection, daemon=True).start()
        except Exception as e:
            print("Error in listener: ", e)
            break
    sckt.close()



def sender(db:DataBase):
    print("Sender thread started")
    connection_cache = {}

    while True:
        try:
            msg, host, port = send_queue.get()
            payload = msg.serialize() + b'\n'
            key = (host, port)
            is_hb = msg.getType() == Message.ADD_NEIGHBOUR
            max_retries = 1 if is_hb else MAX_RETRIES
            timeout_s = 0.5 if is_hb else 5.0
            success = False

            for attempt in range(max_retries):
                sckt = None
                try:
                    if not is_hb and key in connection_cache and attempt == 0:
                        sckt = connection_cache[key]
                        sckt.sendall(payload)
                        success = True
                        break

                    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sckt.settimeout(timeout_s)
                    sckt.connect((host, port))
                    _send_buffer(sckt, payload)
                    if not is_hb:
                        connection_cache[key] = sckt
                    success = True
                    break

                except Exception as e:
                    if key in connection_cache:
                        try:
                            connection_cache[key].close()
                        except:
                            pass
                        connection_cache.pop(key, None)
                    if sckt:
                        try:
                            sckt.close()
                        except:
                            pass
                    if attempt < max_retries - 1:
                        time.sleep(0.2)
                        continue

        except Exception as e:
            log_ev("SENDER_ERR", err=e)
            time.sleep(0.5)
            continue

def ping_handler(msg: Message, db: DataBase):
    try:
        payload = json.loads(msg.data or "{}")
    except Exception:
        return
    stream_id = payload.get("stream_id")
    ttl = int(payload.get("ttl", 0))
    path = payload.get("path", [])
    if not stream_id or ttl < 0:
        return
    path.append(db.my_ip)
    parent = _upstream_for(stream_id, db)
    if parent and ttl > 0:
        fwd = Message(Message.PING, db.get_my_ip(parent), json.dumps({
            "stream_id": stream_id,
            "ttl": ttl - 1,
            "path": path
        }))
        send_message(fwd, parent, ROUTERS_RECEIVER_PORT)
        print(f"FWD stream={stream_id} ttl={ttl-1} -> {parent} path={path}")
    else:
        print(f"END stream={stream_id} path={path}")
            

def handle_control_client(client_socket, client_address, db: DataBase):
    try:
        client_socket.settimeout(10.0)
        buffer = b""
        while True:
            try:
                data = client_socket.recv(4096)
            except socket.timeout:
                break
            except Exception as e:
                break

            if not data:
                break
            
            buffer += data

            while b'\n' in buffer:
                msg_bytes, buffer = buffer.split(b'\n', 1)
                if not msg_bytes:
                    continue

                try:
                    msg = Message.deserialize(msg_bytes)
                except Exception as e:
                    print(f"Error deserializing message: {e}")
                    continue

                db.touch_neighbor(client_address[0])

                try:
                    if msg.getType() == Message.ADD_NEIGHBOUR:
                        resp = Message(Message.RESP_NEIGHBOUR, db.get_my_ip(client_address[0]), "")
                        send_message(resp, client_address[0], ROUTERS_RECEIVER_PORT)
                    elif msg.getType() == Message.VIDEO_METRIC_REQUEST:
                        metric_request_handler(msg, db)
                    elif msg.getType() == Message.VIDEO_METRIC_RESPONSE:
                        metric_response_handler(msg, db)
                    elif msg.getType() == Message.VIDEO_METRIC_UPDATE:
                        metric_update_handler(msg, db)
                    elif msg.getType() == Message.PING:
                        ping_handler(msg, db)
                    elif msg.getType() == Message.STREAMS_AVAILABLE:
                        streams = db.get_streams()
                        data = ",".join(streams) if streams else "No streams available"
                        resp = Message(Message.RESP_STREAMS_AVAILABLE, db.get_my_ip(client_address[0]), data)
                        _send_buffer(client_socket, resp.serialize() + b'\n')
                    elif msg.getType() == Message.STREAM_REQUEST:
                        stream_pls_handler(msg, db)
                        resp = Message(Message.STREAM_REQUEST, db.get_my_ip(client_address[0]), "OK")
                        _send_buffer(client_socket, resp.serialize() + b'\n')
                    elif msg.getType() == Message.STREAM_STOP:
                        stream_no_handler(msg, db)
                        resp = Message(Message.STREAM_STOP, db.get_my_ip(client_address[0]), "OK")
                        _send_buffer(client_socket, resp.serialize() + b'\n')
                except Exception as e:
                    print(f"Error handling message type {msg.getType()}: {e}")
                    import traceback
                    traceback.print_exc()
    
    except Exception as e:
        print(f"Connection handler error: {e}")
    finally:
        try:
            client_socket.close()
        except:
            pass

def cntrl(db:DataBase):
    print("Control thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind((db.my_ip, ROUTERS_RECEIVER_PORT))
    sckt.listen(10)

    while True:
        try:
            client_socket, client_address = sckt.accept()
            t = threading.Thread(target=handle_control_client, args=(client_socket, client_address, db), daemon=True)
            t.start()
        except Exception as e:
            print(f"Accept error: {e}")
            time.sleep(1)
            continue
    

def _store_frame(stream_id: str, frame_num: int, frame_data: bytes):
    """Armazena frame em buffer simples por stream, limitado a FRAME_BUFFER_SIZE."""
    with _frame_lock:
        frames = _frame_buffer.setdefault(stream_id, {})
        if len(frames) >= FRAME_BUFFER_SIZE:
            oldest = min(frames.keys())
            frames.pop(oldest, None)
        frames[frame_num] = frame_data


def forward_mm(raw_packet: bytes, stream_id: str, sender_ip: str, db: DataBase, sock: socket.socket = None):
    """Replica o pacote MM para vizinhos downstream ativos, exceto quem enviou."""
    try:
        downstream = [viz for viz in db.get_downstream(stream_id) if viz != sender_ip]
    except Exception:
        downstream = []

    if not downstream:
        return

    close_sock = False
    if sock is None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        close_sock = True

    for viz in downstream:
        try:
            sock.sendto(raw_packet, (viz, SENDER_PORT))
            log_ev("MM_FWD", stream=stream_id, to=viz, from_=sender_ip)
        except Exception as e:
            log_ev("MM_FWD_ERR", stream=stream_id, to=viz, err=e)
    
    if close_sock:
        sock.close()


def _normalize_stream(stream_id: str, origin: Optional[str]) -> str:
    return f"{origin}:{stream_id}" if origin else stream_id

def announce_handler(msg: Message, db: DataBase):
    try:
        data = json.loads(msg.data)
        stream_id = data.get("stream_id")
        origin = data.get("origin", "")
        cost = float(data.get("cost", 0))
        ttl = int(data.get("ttl", 0))
        msg_id = data.get("msg_id")
    except Exception as e:
        print(f"Error parsing announce message: {e}")
        return

    if not stream_id or not msg_id or ttl < 0:
        return

    stream_key = _normalize_stream(stream_id, origin)

    if db.mark_seen(msg_id, stream_key):
        return

    sender_ip = msg.src
    db.touch_neighbor(sender_ip)
    new_cost = cost + 1
    db.hysteresis_factor = HYST_FACTOR
    improved = db.update_announce(stream_key, new_cost, sender_ip)
    if improved:
        log_ev("ANN_UPD", stream=stream_key, parent=sender_ip, cost=new_cost)
        if stream_key not in db.available_streams:
            db.addStream(stream_key, sender_ip)

    if ttl > 0 and improved:
        fwd_payload = json.dumps({
            "stream_id": stream_id,
            "origin": origin,
            "cost": new_cost,
            "ttl": ttl - 1,
            "msg_id": msg_id
        })
        for viz in db.get_vizinhos():
            if viz == sender_ip:
                continue
            m = Message(ANNOUNCE_TYPE, db.my_ip, fwd_payload)
            send_message(m, viz, ROUTERS_RECEIVER_PORT)
            log_ev("ANN_FWD", stream=stream_key, cost=new_cost, ttl=ttl-1, to=viz)

def measure_rtt(host: str, port: int = ROUTERS_RECEIVER_PORT, timeout: float = 1.0) -> float:
    """Mede RTT aproximado até host:port em ms; retorna valor alto em caso de erro."""
    try:
        t0 = time.time()
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return (time.time() - t0) * 1000.0
    except Exception:
        return 1_000_000.0

def heartbeat_sender(db: DataBase):
    """Envia heartbeats leves aos vizinhos para manter last_seen atualizado."""
    while True:
        for viz in db.get_vizinhos():
            msg = Message(Message.ADD_NEIGHBOUR, db.get_my_ip(viz), "")
            send_message(msg, viz, ROUTERS_RECEIVER_PORT)
        time.sleep(HEARTBEAT_INTERVAL)

_stream_last_seen = {}
_stream_last_seen_lock = threading.Lock()

def heartbeat_check(db: DataBase):
    """Verifica timeouts de vizinhos, se offline, invalida parents e streams."""
    while True:
        dead = []
        for viz in db.get_vizinhos():
            if not db.is_neighbor_alive(viz, HEARTBEAT_TIMEOUT):
                dead.append(viz)
        if dead:
            affected_streams = set()
            with db.lock:
                for viz in dead:
                    if viz in db.vizinhos_inicializados:
                        db.vizinhos_inicializados[viz] = 0

                    for stream_id, ds in list(db.downstream.items()):
                        if viz in ds:
                            ds.discard(viz)
                            if not ds:
                                db.downstream.pop(stream_id, None)
                            affected_streams.add(stream_id)

                    if viz in db.active_streams_table:
                        db.active_streams_table[viz].clear()

                    to_reset = [s for s, p in db.best_parent.items() if p == viz]
                    for s in to_reset:
                        db.best_parent.pop(s, None)
                        db.best_cost[s] = math.inf  # reset para aceitar anúncio seguinte mesmo com custo maior
                        affected_streams.add(s)

                    # log_ev("HB_NEIGH_DOWN", viz=viz, parents_reset=to_reset)

            for stream_id in affected_streams:
                parent = _upstream_for(stream_id, db)
                if parent and parent not in dead:
                    msg = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
                    send_message(msg, parent, RECEIVER_PORT)
                    log_ev("HB_REROUTE", stream=stream_id, new_parent=parent)
        
        now = time.time()
        streams_to_check = []
        with db.lock:
            for stream_id in db.downstream.keys():
                if db.has_downstream(stream_id):
                    streams_to_check.append(stream_id)
        
        for stream_id in streams_to_check:
            with _stream_last_seen_lock:
                last = _stream_last_seen.get(stream_id, 0)
            
            if now - last > 2.0:
                parent = _upstream_for(stream_id, db)
                if parent:
                    msg = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
                    send_message(msg, parent, RECEIVER_PORT)
                    with _stream_last_seen_lock:
                        _stream_last_seen[stream_id] = now 

        time.sleep(HEARTBEAT_INTERVAL)



# =============================================================
#                      PRINCIPAL THREADS
# =============================================================

def data_listener(db: DataBase):
    """Escuta pacotes de dados via UDP (SimplePacket) e faz forwarding conforme downstream ativos."""
    print("Data listener thread started (UDP)")
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024) # Increase buffer to 1MB
    udp_sock.bind((db.my_ip, SENDER_PORT))

    _last_correction_time = {}

    while True:
        try:
            raw, addr = udp_sock.recvfrom(65535)
            sender_ip = addr[0]

            try:
                pkt = SimplePacket.decode(raw)
            except Exception as e:
                print(f"Error decoding packet from {sender_ip}: {e}")
                continue

            stream_num = pkt.get_stream_id()
            frame_num = pkt.get_frame_num()
            payload = pkt.get_payload()

            if b'\0' not in payload:
                print(f"Error malformed payload from {sender_ip}")
                continue
            stream_id_bytes, batch_data = payload.split(b'\0', 1)
            try:
                stream_id = stream_id_bytes.decode('utf-8')  # já pode ser "S1:stream3"
            except Exception:
                print(f"Error decoding stream_id bytes from {sender_ip}")
                continue

            with _stream_last_seen_lock:
                _stream_last_seen[stream_id] = time.time()

            db.touch_neighbor(sender_ip)

            upstream = db.get_best_parent(stream_id) or db.getStreamSource(stream_id)
            if upstream and upstream != sender_ip:

                now = time.time()
                last_time = _last_correction_time.get((stream_id, sender_ip), 0)
                if now - last_time > 1.0: # Rate limit de 1 segundo
                    _last_correction_time[(stream_id, sender_ip)] = now

                    stop_msg = Message(Message.STREAM_STOP, db.get_my_ip(sender_ip), stream_id)
                    send_message(stop_msg, sender_ip, RECEIVER_PORT)
                    
                    req_msg = Message(Message.STREAM_REQUEST, db.get_my_ip(upstream), stream_id)
                    send_message(req_msg, upstream, RECEIVER_PORT)

                pass


            try:
                if len(batch_data) < 1:
                     raise ValueError("Empty batch")
                count = struct.unpack("!B", batch_data[:1])[0]

                try:
                    downstream_active = db.has_downstream(stream_id)
                except Exception:
                    downstream_active = False

                if not downstream_active:
                    offset = 1
                    for i in range(count):
                        if offset + 4 > len(batch_data): break
                        f_len = struct.unpack("!I", batch_data[offset:offset+4])[0]
                        offset += 4
                        if offset + f_len > len(batch_data): break
                        frame_b = batch_data[offset:offset+f_len]
                        offset += f_len
                        _store_frame(stream_id, frame_num + i, frame_b)

            except Exception as e:
                print(f"Error decoding batch from {sender_ip}: {e}")

            forward_mm(raw, stream_id, sender_ip, db, udp_sock)

        except Exception as e:
            print(f"Error in data listener: {e}")
            continue
def main():
    sys.stdout.write(f"\033]0;{socket.gethostname()}\007")
    sys.stdout.flush()

    if len(sys.argv) < 2:
        sys.exit(1)
    db = DataBase(sys.argv[1])

    thread_listen = threading.Thread(target=listener, args=(db,))
    thread_sender = threading.Thread(target=sender, args=(db,))
    thread_cntrl = threading.Thread(target=cntrl, args=(db,))
    thread_data = threading.Thread(target=data_listener, args=(db,))
    thread_hb = threading.Thread(target=heartbeat_sender, args=(db,))
    thread_hbchk = threading.Thread(target=heartbeat_check, args=(db,))

    all_threads = [thread_listen, thread_sender, thread_cntrl, thread_data, thread_hb, thread_hbchk]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

    


if __name__ == '__main__':
    main()
