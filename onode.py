import socket
import sys
import threading
import queue
import datetime as dt
import json
from database import DataBase
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334

MAX_RETRIES = 3

send_queue = queue.Queue()

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

def stream_pls_handler(msg:Message, db:DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    need_stream = db.activateStream(ip_viz, stream_id)
    print(f"Processed STREAM_PLS for stream {stream_id} from {msg.getSrc()}.\n")
    if need_stream:
        stream_request_handler(stream_id, db)

def stream_no_handler(msg:Message, db:DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    is_active = db.deactivateStream(ip_viz, stream_id)
    print(f"Processed VIDEO_NO for stream {stream_id} from {msg.getSrc()}.\n")
    if not is_active:
        stream_stop_handler(stream_id, db)

def stream_request_handler(stream_id, db:DataBase):
    streamOrigin = db.getStreamSource(stream_id)
    if streamOrigin:
        msg = Message(Message.STREAM_REQUEST, db.get_my_ip(streamOrigin), stream_id)
        send_message(msg, streamOrigin, RECEIVER_PORT)

def stream_stop_handler(stream_id, db:DataBase):
    streamOrigin = db.getStreamSource(stream_id)
    if streamOrigin:
        msg = Message(Message.STREAM_STOP, db.get_my_ip(streamOrigin), stream_id)
        send_message(msg, streamOrigin, RECEIVER_PORT)
    
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
    except Exception:
        return
    request_id = payload.get("request_id")
    start_time = payload.get("start_time")
    streams = payload.get("streams", [])
    
    if not request_id or not start_time:
        return
    
    # Armazena requisição
    db.store_metric_request(request_id, {
        "start_time": start_time,
        "streams": streams,
        "src": msg.getSrc()
    })
    
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
        print(f"[ONODE][METRIC_PROPAGATE] request_id={request_id} -> {list(upstream_neighbors)}")
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
        print(f"[ONODE][METRIC_ORIGIN] request_id={request_id}")

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
    incoming_delay = payload.get("accumulated_delay_ms", 0)
    
    if not request_id:
        print("[ONODE][METRIC_RESP] Missing request_id")
        return
    
    stored = db.get_metric_request(request_id)
    if not stored:
        print(f"[ONODE][METRIC_RESP] No stored request for {request_id}")
        return
    
    start_time = stored.get("start_time")
    if not isinstance(start_time, dt.datetime):
        try:
            start_time = dt.datetime.fromisoformat(start_time)
        except Exception:
            print(f"[ONODE][METRIC_RESP] Invalid start_time format")
            return
    
    # Calcula delay local (RTT até aqui)
    local_delay_ms = (dt.datetime.now() - start_time).total_seconds() * 1000
    
    # Acumula com delay do vizinho downstream
    total_delay_ms = local_delay_ms + incoming_delay
    
    # Atualiza métricas locais
    streams = stored.get("streams", [])
    db.AtualizaMetricas(msg.getSrc(), streams, total_delay_ms)
    
    # Propaga resposta para o nó que iniciou a requisição
    update_msg = Message(
        Message.VIDEO_METRIC_RESPONSE, 
        db.get_my_ip(stored["src"])
    )
    # Mantém o start_time original (não a string ISO)
    update_msg.metrics_encode(
        streams,
        request_id=request_id,
        start_time=start_time,  # Usa datetime original, não a string
        accumulated_delay_ms=total_delay_ms
    )
    
    send_message(update_msg, stored["src"], ROUTERS_RECEIVER_PORT)
    
    db.remove_metric_request(request_id)
    print(f"[ONODE][METRIC_RESP] request_id={request_id} local={local_delay_ms:.2f}ms incoming={incoming_delay:.2f}ms total={total_delay_ms:.2f}ms -> {stored['src']}")

def metric_update_handler(msg: Message, db: DataBase):
    try:
        payload = json.loads(msg.getData() or "{}")
    except json.JSONDecodeError:
        return
    streams = payload.get("streams") or []
    delay_ms = payload.get("delay_ms")
    if streams and delay_ms is not None:
        db.AtualizaMetricas(msg.getSrc(), streams, delay_ms)
        print(f"[ONODE][METRIC_UPDATE] from={msg.getSrc()} streams={streams} delay_ms={delay_ms}")


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
            print(f"[ONODE][SENDER] dequeued type={msg.getType()} target={host}:{port}")
            payload = msg.serialize() + b'\n'
            key = (host, port)
            success = False
            
            for attempt in range(MAX_RETRIES):
                try:

                    if key in connection_cache and attempt == 0:
                        sckt = connection_cache[key]
                        try:
                            sckt.sendall(msg.serialize() + b'\n')
                            success = True
                            break
                        except Exception:
                            try:
                                sckt.close()
                            except:
                                pass
                            del connection_cache[key]
                    
                    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sckt.settimeout(5.0)
                    sckt.connect((host, port))
                    _send_buffer(sckt, payload)
                    connection_cache[key] = sckt
                    success = True
                    break
                    
                except Exception as e:
                    if key in connection_cache:
                        try:
                            connection_cache[key].close()
                        except:
                            pass
                        del connection_cache[key]
                    
                    if attempt < MAX_RETRIES - 1:
                        print(f"Attempt {attempt + 1} failed for {host}:{port}, retrying...")
                    else:
                        print(f"ERROR: Failed to send message to {host}:{port} after {MAX_RETRIES} attempts - {e}")
                        print(f"Node {host} may be offline.")
            
            if not success:
                print(f"Message to {host}:{port} discarded after {MAX_RETRIES} failed attempts.")
                    
        except Exception as e:
            print("Error in sender: ", e)
            break
    
    for sckt in connection_cache.values():
        try:
            sckt.close()
        except:
            pass
            

def cntrl(db:DataBase):
    """
    avisa os vizinhos, que está ligado, verifica quais estão e quando um é desligado ele percebe com o tempo
    """
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', ROUTERS_RECEIVER_PORT))
    sckt.listen(10)

    vizinhos = db.get_vizinhos()
    threading.Thread(target=wake_router_handler, args=(vizinhos, db)).start()

    while True:
        try:
            conn, addr = sckt.accept()
            
            def handle_router_connection():
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
                                if typeOfMsg == Message.ADD_NEIGHBOUR:
                                    print(f"[ONODE][CNTRL] ADD_NEIGHBOUR from {msgr_ip}")
                                    threading.Thread(target=db.inicializaVizinho, args=(msgr_ip,)).start()
                                    msg_resp = Message(Message.RESP_NEIGHBOUR, db.get_my_ip(msgr_ip), "")

                                    success = False
                                    for attempt in range(MAX_RETRIES):
                                        try:
                                            _send_buffer(conn, msg_resp.serialize() + b'\n')
                                            success = True
                                            break
                                        except Exception as e:
                                            if attempt < MAX_RETRIES - 1:
                                                print(f"Attempt {attempt + 1} failed to send RESP_NEIGHBOUR to {msgr_ip}, retrying...")
                                            else:
                                                print(f"ERROR: Failed to send RESP_NEIGHBOUR to {msgr_ip} after {MAX_RETRIES} attempts - {e}")
                                    
                                    if not success:
                                       print(f"RESP_NEIGHBOUR to {msgr_ip} discarded after {MAX_RETRIES} failed attempts.")
                                       break 
                                elif typeOfMsg == Message.RESP_NEIGHBOUR:
                                    threading.Thread(target=db.inicializaVizinho, args=(msgr_ip,)).start()
                                elif typeOfMsg == Message.VIDEO_METRIC_REQUEST:
                                    threading.Thread(target=metric_request_handler, args=(msg, db)).start()
                                elif typeOfMsg == Message.VIDEO_METRIC_RESPONSE:
                                    threading.Thread(target=metric_response_handler, args=(msg, db)).start()
                                elif typeOfMsg == Message.VIDEO_METRIC_UPDATE:
                                    threading.Thread(target=metric_update_handler, args=(msg, db)).start()
                except Exception as e:
                    print("Error in router connection: ", e)
                finally:
                    conn.close()
            
            threading.Thread(target=handle_router_connection, daemon=True).start()
        except Exception as e:
            print("Error in listener: ", e)
            break
    sckt.close()


def main():
    sys.stdout.write(f"\033]0;{socket.gethostname()}\007")
    sys.stdout.flush()

    if len(sys.argv) < 2:
        sys.exit(1)
    db = DataBase(sys.argv[1])

    thread_listen = threading.Thread(target=listener, args=(db,))
    thread_sender = threading.Thread(target=sender, args=(db,))
    thread_cntrl = threading.Thread(target=cntrl, args=(db,))

    all_threads = [thread_listen, thread_sender, thread_cntrl]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

    


if __name__ == '__main__':
    main()
