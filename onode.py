import socket
import sys
import threading
import queue
from database import DataBase
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334

MAX_RETRIES = 3

send_queue = queue.Queue()

def send_message(msg:Message, host:str, port:int):
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
        msg = Message(Message.STREAM_PLS, db.get_my_ip(streamOrigin), stream_id)
        send_message(msg, streamOrigin, RECEIVER_PORT)

def stream_stop_handler(stream_id, db:DataBase):
    streamOrigin = db.getStreamSource(stream_id)
    if streamOrigin:
        msg = Message(Message.VIDEO_NO, db.get_my_ip(streamOrigin), stream_id)
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
                    sckt.sendall(msg.serialize() + b'\n')
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
                                    threading.Thread(target=db.inicializaVizinho, args=(msgr_ip,)).start()
                                    msg_resp = Message(Message.RESP_NEIGHBOUR, db.get_my_ip(msgr_ip), "")

                                    success = False
                                    for attempt in range(MAX_RETRIES):
                                        try:
                                            conn.sendall(msg_resp.serialize() + b'\n')
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
                                elif typeOfMsg == Message.VIDEO_METRIC:
                                    data = msg.parseStringfy(msg.getData())
                                    threading.Thread(target=db.update_metrics, args=(msg,)).start()
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
