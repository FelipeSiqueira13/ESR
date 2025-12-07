import socket
import sys
import threading
import queue
import datetime as dt
import json, uuid
import math
import time
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

HEARTBEAT_INTERVAL = 5      # segundos
HEARTBEAT_TIMEOUT  = 15     # segundos
HYST_FACTOR = 0.9           # mesma ideia do DB; opcional sobrescrever


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
    print(f"[ONODE][QUEUE] type={msg.getType()} -> {host}:{port} data={msg.getData()}")
    send_queue.put((msg, host, port))

def _upstream_for(stream_id: str, db: DataBase):
    """Escolhe o vizinho upstream (best_parent); fallback para origem conhecida."""
    parent = db.get_best_parent(stream_id)
    if parent:
        return parent
    return db.getStreamSource(stream_id)

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
            print(f"[ONODE][STREAM_REQ] stream={stream_id} -> {parent}")
        else:
            print(f"[ONODE][STREAM_REQ][NO_PARENT] stream={stream_id}")

def stream_no_handler(msg: Message, db: DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    still_active = db.deactivateStream(ip_viz, stream_id)
    db.set_downstream_active(ip_viz, stream_id, False)
    print(f"Processed VIDEO_NO for stream {stream_id} from {msg.getSrc()}.\n")
    if not still_active:
        parent = _upstream_for(stream_id, db)
        if parent:
            stop = Message(Message.STREAM_STOP, db.get_my_ip(parent), stream_id)
            send_message(stop, parent, RECEIVER_PORT)
            print(f"[ONODE][STREAM_STOP] stream={stream_id} -> {parent}")
        else:
            print(f"[ONODE][STREAM_STOP][NO_PARENT] stream={stream_id}")

def stream_request_handler(stream_id, db: DataBase):
    """Mantido para compatibilidade, agora usa best_parent."""
    parent = _upstream_for(stream_id, db)
    if parent:
        msg = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
        send_message(msg, parent, RECEIVER_PORT)
        print(f"[ONODE][STREAM_REQ] stream={stream_id} -> {parent}")

def stream_stop_handler(stream_id, db: DataBase):
    parent = _upstream_for(stream_id, db)
    if parent:
        msg = Message(Message.STREAM_STOP, db.get_my_ip(parent), stream_id)
        send_message(msg, parent, RECEIVER_PORT)
        print(f"[ONODE][STREAM_STOP] stream={stream_id} -> {parent}")

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


def metric_request_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"[ONODE][METRIC_REQ] Error decoding metrics: {e}")
        import traceback
        traceback.print_exc()
        return

    request_id = payload.get("request_id")
    start_time = payload.get("start_time")
    streams = payload.get("streams", [])
    
    if not request_id or not start_time:
        return

    log_ev("METRIC_REQ_RECV", req=request_id, streams=streams, cost=None, parent=None, from_=msg.getSrc())
    
    # Armazena requisição
    db.store_metric_request(request_id, {
        "start_time": start_time,
        "streams": streams,
        "src": msg.getSrc()
    })
    
    # Verifica se a mensagem veio de UPSTREAM (Server -> Node)
    # Se o remetente for a origem da stream, então é um fluxo de descida (Server push)
    is_downstream_flow = False
    for stream in streams:
        origin = db.getStreamSource(stream)
        if origin == msg.getSrc():
            is_downstream_flow = True
            break
    
    if is_downstream_flow:
        # Fluxo Server -> Node -> Node
        # Calcula delay acumulado até aqui
        current_delay = (dt.datetime.now() - start_time).total_seconds() * 1000
        
        # Atualiza métricas locais
        db.AtualizaMetricas(msg.getSrc(), streams, current_delay)
        log_ev("METRIC_REQ_PROCESS", req=request_id, streams=streams, delay=current_delay, from_=msg.getSrc())

        # Propaga para vizinhos DOWNSTREAM (quem está recebendo a stream)
        downstream_neighbors = set()
        with db.lock:
            for stream in streams:
                for viz, active_streams in db.active_streams_table.items():
                    if active_streams.get(stream) == 1 and viz != msg.getSrc():
                        downstream_neighbors.add(viz)
        
        if downstream_neighbors:
            for downstream in downstream_neighbors:
                fwd_msg = Message(Message.VIDEO_METRIC_REQUEST, db.get_my_ip(downstream))
                # Mantém start_time original para medir latência total desde a origem
                # Passa current_delay como accumulated_delay_ms para informação
                fwd_msg.metrics_encode(
                    streams,
                    request_id=request_id,
                    start_time=start_time,
                    accumulated_delay_ms=current_delay
                )
                send_message(fwd_msg, downstream, ROUTERS_RECEIVER_PORT)
                log_ev("METRIC_REQ_FWD_DS", req=request_id, to=downstream, delay=current_delay)
        else:
            log_ev("METRIC_REQ_END", req=request_id, msg="No downstream neighbors")

    else:
        # Fluxo Client -> Node -> Server (Lógica original)
        # CORRETO: Propaga para vizinhos UPSTREAM (origem das streams)
        upstream_neighbors = set()
        for stream in streams:
            origin = db.getStreamSource(stream)
            if origin and origin != msg.getSrc():
                upstream_neighbors.add(origin)
        
        if upstream_neighbors:
            # Propaga para nós upstream
            for neighbor in upstream_neighbors:
                fwd_msg = Message(Message.VIDEO_METRIC_REQUEST, db.get_my_ip(neighbor))
                fwd_msg.metrics_encode(
                    streams,
                    request_id=request_id,
                    start_time=start_time,
                    accumulated_delay_ms=0
                )
                send_message(fwd_msg, neighbor, ROUTERS_RECEIVER_PORT)
                log_ev("METRIC_REQ_FWD_US", req=request_id, streams=streams, cost=None, parent=None, to=neighbor, from_=db.get_my_ip(neighbor))
        else:
            # Nó origem: responde imediatamente
            response = Message(Message.VIDEO_METRIC_RESPONSE, db.get_my_ip(msg.getSrc()))
            response.metrics_encode(
                streams,
                request_id=request_id,
                start_time=start_time,
                accumulated_delay_ms=0
            )
            send_message(response, msg.getSrc(), ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_REQ_ORIGIN", req=request_id, streams=streams, cost=0, parent=None, to=msg.getSrc())

def metric_response_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"[ONODE][METRIC_RESP] Failed to decode: {e}")
        return
    
    if not isinstance(payload, dict):
        print(f"[ONODE][METRIC_RESP] Invalid payload type: {type(payload)}")
        return
    
    request_id = payload.get("request_id")
    incoming_delay = float(payload.get("accumulated_delay_ms", 0))
    streams = payload.get("streams", [])
    start_time = payload.get("start_time")
    
    if not request_id or not streams:
        print("[ONODE][METRIC_RESP] Missing request_id or streams")
        return
    
    # RTT local até quem enviou a resposta
    local_rtt_ms = measure_rtt(msg.getSrc())
    total_delay_ms = incoming_delay + local_rtt_ms

    log_ev("METRIC_RESP_RECV", req=request_id, streams=streams, cost=total_delay_ms, parent=msg.getSrc(), from_=msg.getSrc(), incoming_ms=incoming_delay, rtt_ms=local_rtt_ms, total_ms=total_delay_ms)

    # Atualiza métricas e possivelmente best_parent para cada stream
    db.AtualizaMetricas(msg.getSrc(), streams, total_delay_ms)
    for stream in streams:
        db.update_announce(stream, total_delay_ms, msg.getSrc())

    stored = db.get_metric_request(request_id)
    
    if stored:
        # Nó originador da requisição
        db.remove_metric_request(request_id)
        log_ev("METRIC_RESP_ORIGIN", req=request_id, streams=streams, cost=total_delay_ms, parent=msg.getSrc(), total_ms=total_delay_ms, rtt_ms=local_rtt_ms)
        return

    # Nó intermediário: propaga downstream (quem pediu/está ativo)
    downstream_neighbors = set()
    with db.lock:
        for stream in streams:
            for viz, active_streams in db.active_streams_table.items():
                if active_streams.get(stream) == 1 and viz != msg.getSrc():
                    downstream_neighbors.add(viz)

    if downstream_neighbors:
        for downstream in downstream_neighbors:
            fwd_msg = Message(Message.VIDEO_METRIC_RESPONSE, db.get_my_ip(downstream))
            fwd_msg.metrics_encode(
                streams,
                request_id=request_id,
                start_time=start_time,
                accumulated_delay_ms=total_delay_ms
            )
            send_message(fwd_msg, downstream, ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_RESP_FWD", req=request_id, streams=streams, cost=total_delay_ms, parent=msg.getSrc(), to=downstream, total_ms=total_delay_ms, from_=msg.getSrc())
    else:
        log_ev("METRIC_RESP_NO_DS", req=request_id, streams=streams)

def metric_update_handler(msg: Message, db: DataBase):
    try:
        payload = msg.metrics_decode()
    except Exception as e:
        print(f"[ONODE][METRIC_UPDATE] Failed to decode: {e}")
        return
    
    if not isinstance(payload, dict):
        print(f"[ONODE][METRIC_UPDATE] Invalid payload type")
        return
    
    streams = payload.get("streams", [])
    accumulated_delay = payload.get("accumulated_delay_ms", 0)
    request_id = payload.get("request_id")
    
    if not streams:
        print("[ONODE][METRIC_UPDATE] Missing streams")
        return
    
    # Atualiza métricas locais com o delay acumulado
    db.AtualizaMetricas(msg.getSrc(), streams, accumulated_delay)
    log_ev("METRIC_UPDATE_RECV", req=request_id, streams=streams, cost=accumulated_delay, parent=msg.getSrc(), delay_ms=accumulated_delay, from_=msg.getSrc())
    
    # Propaga o UPDATE para vizinhos downstream (que não são a origem)
    stored = db.get_metric_request(request_id) if request_id else None
    
    if stored:
        # Se temos a requisição original, propaga para quem pediu
        downstream = stored.get("src")
        if downstream and downstream != msg.getSrc():
            update_msg = Message(Message.VIDEO_METRIC_UPDATE, db.get_my_ip(downstream))
            update_msg.metrics_encode(
                streams,
                request_id=request_id,
                start_time=stored.get("start_time"),
                accumulated_delay_ms=accumulated_delay
            )
            send_message(update_msg, downstream, ROUTERS_RECEIVER_PORT)
            log_ev("METRIC_UPDATE_FWD", req=request_id, streams=streams, cost=accumulated_delay, parent=msg.getSrc(), delay_ms=accumulated_delay, to=downstream, from_=msg.getSrc())
        
        # Remove requisição processada
        db.remove_metric_request(request_id)
    else:
        # Se não temos requisição armazenada, este update veio de outra fonte
        # Propaga para todos os vizinhos downstream ativos para estas streams
        with db.lock:
            downstream_neighbors = set()
            for stream in streams:
                # Encontra vizinhos que estão recebendo esta stream
                for viz, active_streams in db.active_streams_table.items():
                    if active_streams.get(stream) == 1 and viz != msg.getSrc():
                        downstream_neighbors.add(viz)
            
            for downstream in downstream_neighbors:
                update_msg = Message(Message.VIDEO_METRIC_UPDATE, db.get_my_ip(downstream))
                update_msg.metrics_encode(
                    streams,
                    request_id=request_id or f"fwd-{int(dt.datetime.now().timestamp()*1000)}",
                    start_time=dt.datetime.now(),
                    accumulated_delay_ms=accumulated_delay
                )
                send_message(update_msg, downstream, ROUTERS_RECEIVER_PORT)
                log_ev("METRIC_UPDATE_BCAST", req=request_id, streams=streams, cost=accumulated_delay, parent=msg.getSrc(), delay_ms=accumulated_delay, to=downstream, from_=msg.getSrc())


# =============================================================
#                      PRINCIPAL THREADS
# =============================================================

def listener(db:DataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', RECEIVER_PORT))
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
                                print(f"[ONODE][LISTENER] from={addr[0]} type={typeOfMsg} data={msg.getData()}")
                                if typeOfMsg == Message.STREAM_REQUEST:
                                    threading.Thread(target=stream_pls_handler, args=(msg, db)).start()
                                elif typeOfMsg == Message.STREAM_STOP:
                                    threading.Thread(target=stream_no_handler, args=(msg, db)).start()
                                elif typeOfMsg == Message.STREAMS_AVAILABLE:
                                    threading.Thread(target=streams_available_handler, args=(msg, db)).start()
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
            success = False

            for attempt in range(MAX_RETRIES):
                sckt = None
                try:
                    # tenta usar cache
                    if key in connection_cache and attempt == 0:
                        sckt = connection_cache[key]
                        sckt.sendall(payload)
                        success = True
                        log_ev("TX_OK", type=msg.getType(), host=host, port=port, cached=True)
                        break

                    # nova conexão
                    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sckt.settimeout(5.0)
                    sckt.connect((host, port))
                    _send_buffer(sckt, payload)
                    connection_cache[key] = sckt
                    success = True
                    log_ev("TX_OK", type=msg.getType(), host=host, port=port, cached=False)
                    break

                except Exception as e:
                    log_ev("TX_FAIL", type=msg.getType(), host=host, port=port, attempt=attempt+1, err=e)
                    # limpa cache se estava guardado
                    if key in connection_cache:
                        try:
                            connection_cache[key].close()
                        except:
                            pass
                        connection_cache.pop(key, None)
                    # fecha socket temporário
                    if sckt:
                        try:
                            sckt.close()
                        except:
                            pass
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(0.2)
                        continue
                # fim try/except
            # fim for

            if not success:
                log_ev("TX_DROP", type=msg.getType(), host=host, port=port, retries=MAX_RETRIES)

        except Exception as e:
            log_ev("SENDER_ERR", err=e)
            # pequeno backoff para não travar em loop de erros
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
        print(f"[PING][FWD] stream={stream_id} ttl={ttl-1} -> {parent} path={path}")
    else:
        print(f"[PING][END] stream={stream_id} path={path}")
            

def handle_control_client(client_socket, client_address, db: DataBase):
    try:
        client_socket.settimeout(10.0)
        buffer = b""
        while True:
            try:
                data = client_socket.recv(4096)
            except socket.timeout:
                # print(f"[ONODE][CNTRL] Timeout reading from {client_address}")
                break
            except Exception as e:
                # print(f"[ONODE][CNTRL] Recv error from {client_address}: {e}")
                break

            if not data:
                break
            
            buffer += data

            # Processa buffer procurando por delimitador \n
            while b'\n' in buffer:
                msg_bytes, buffer = buffer.split(b'\n', 1)
                if not msg_bytes:
                    continue

                try:
                    msg = Message.deserialize(msg_bytes)
                except Exception as e:
                    print(f"[ONODE][CNTRL] Error deserializing message: {e}")
                    continue

                # marca neighbor vivo em qualquer mensagem de controle
                db.touch_neighbor(client_address[0])

                try:
                    # if msg.getType() == ANNOUNCE_TYPE:
                    #     announce_handler(msg, db)
                    if msg.getType() == Message.ADD_NEIGHBOUR:
                        # simples ack para manter vivo
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
                except Exception as e:
                    print(f"[ONODE][CNTRL] Error handling message type {msg.getType()}: {e}")
                    import traceback
                    traceback.print_exc()
    
    except Exception as e:
        print(f"[ONODE][CNTRL] Connection handler error: {e}")
    finally:
        try:
            client_socket.close()
        except:
            pass

def cntrl(db:DataBase):
    print("Control thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', ROUTERS_RECEIVER_PORT))
    sckt.listen(10)

    while True:
        try:
            client_socket, client_address = sckt.accept()
            t = threading.Thread(target=handle_control_client, args=(client_socket, client_address, db), daemon=True)
            t.start()
        except Exception as e:
            print(f"[ONODE][CNTRL] Accept error: {e}")
            time.sleep(1)
            continue
    

def _store_frame(stream_id: str, frame_num: int, frame_data: bytes):
    """Armazena frame em buffer simples por stream, limitado a FRAME_BUFFER_SIZE."""
    with _frame_lock:
        frames = _frame_buffer.setdefault(stream_id, {})
        # descarte o mais antigo se ultrapassar limite
        if len(frames) >= FRAME_BUFFER_SIZE:
            oldest = min(frames.keys())
            frames.pop(oldest, None)
        frames[frame_num] = frame_data
    print(f"[ONODE][FRAME] stored stream={stream_id} frame={frame_num} size={len(frame_data)}B")


def forward_mm(raw_packet: bytes, stream_id: str, sender_ip: str, db: DataBase):
    """Replica o pacote MM para vizinhos downstream ativos, exceto quem enviou."""
    try:
        downstream = [viz for viz in db.get_downstream(stream_id) if viz != sender_ip]
    except Exception:
        downstream = []

    if not downstream:
        return

    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for viz in downstream:
        try:
            udp_sock.sendto(raw_packet, (viz, SENDER_PORT))
            log_ev("MM_FWD", stream=stream_id, to=viz, from_=sender_ip)
        except Exception as e:
            log_ev("MM_FWD_ERR", stream=stream_id, to=viz, err=e)
    udp_sock.close()


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
        print(f"[ONODE][ANNOUNCE][DROP] parse: {e}")
        return

    if not stream_id or not msg_id or ttl < 0:
        return

    # chave composta
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

def heartbeat_check(db: DataBase):
    """Verifica timeouts de vizinhos; se offline, invalida parents e streams."""
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

                    # remove entradas de downstream e active_streams_table
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

                    log_ev("HB_NEIGH_DOWN", viz=viz, parents_reset=to_reset)

            # Fora do lock: tenta recalc e reenviar STREAM_REQUEST para streams afetadas
            for stream_id in affected_streams:
                parent = _upstream_for(stream_id, db)
                if parent and parent not in dead:
                    msg = Message(Message.STREAM_REQUEST, db.get_my_ip(parent), stream_id)
                    send_message(msg, parent, RECEIVER_PORT)
                    log_ev("HB_REROUTE", stream=stream_id, new_parent=parent)
        time.sleep(HEARTBEAT_INTERVAL)



# =============================================================
#                      PRINCIPAL THREADS
# =============================================================

def data_listener(db: DataBase):
    """Escuta pacotes de dados via UDP (SimplePacket) e faz forwarding conforme downstream ativos."""
    print("Data listener thread started (UDP)")
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind(('', SENDER_PORT))

    while True:
        try:
            raw, addr = udp_sock.recvfrom(65535)
            sender_ip = addr[0]

            try:
                pkt = SimplePacket.decode(raw)
            except Exception as e:
                print(f"[ONODE][MM][DROP] decode fail from {sender_ip}: {e}")
                continue

            stream_num = pkt.get_stream_id()
            frame_num = pkt.get_frame_num()
            payload = pkt.get_payload()

            if b'\0' not in payload:
                print(f"[ONODE][MM][DROP] malformed payload from {sender_ip}")
                continue
            stream_id_bytes, frame_bytes = payload.split(b'\0', 1)
            try:
                stream_id = stream_id_bytes.decode('utf-8')  # já pode ser "S1:stream3"
            except Exception:
                print(f"[ONODE][MM][DROP] bad stream_id bytes from {sender_ip}")
                continue

            # Valida origem esperada (best_parent)
            upstream = db.get_best_parent(stream_id) or db.getStreamSource(stream_id)
            if upstream and upstream != sender_ip:
                print(f"[ONODE][MM][DROP] unexpected upstream for {stream_id}: got {sender_ip}, expect {upstream}")
                continue

            print(f"[ONODE][MM][RX] stream={stream_id} frame={frame_num} from={sender_ip} size={len(frame_bytes)}B")

            # Se não há downstream ativo, assume nó folha/cliente e guarda frame
            try:
                downstream_active = db.has_downstream(stream_id)
            except Exception:
                downstream_active = False

            if not downstream_active:
                _store_frame(stream_id, frame_num, frame_bytes)

            forward_mm(raw, stream_id, sender_ip, db)

        except Exception as e:
            print(f"[ONODE][MM][ERR] {e}")
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
