import threading
import socket
import sys
from server_database import ServerDataBase
from msg import Message
from VideoStream import VideoStream
import time
import json

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334

def stream_request_handler(msg, database: ServerDataBase):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.initiate_stream(src, stream_id)
    print(f"Stream {stream_id} requested by {src}")

def stream_stop_handler(msg, database: ServerDataBase):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.end_stream(src, stream_id)
    print(f"Stream {stream_id} stopped for {src}")

def listener(sdb:ServerDataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', RECEIVER_PORT))
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
                                msgr_ip = msg.getSrc()
                                typeOfMsg = msg.getType()

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

def sender(sdb:ServerDataBase):
    print("Sender thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    streams_active = {}
    while True:
        try:
            with sdb.lock:
                stream_viz_copy = dict(sdb.stream_vizinhos)
            
            for stream_id, viz in stream_viz_copy.items():
                if stream_id not in streams_active:
                    path = sdb.server_streams.get(stream_id)
                    if path:
                        try:
                            streams_active[stream_id] = VideoStream(path)
                        except Exception as e:
                            print(f"Error loading VideoStream for {stream_id}: {e}")
                            continue

                vs = streams_active.get(stream_id)
                if vs:
                    try:
                        frame = vs.nextFrame()
                        for vizinho in viz:
                            src = sdb.get_my_ip(vizinho)
                            msg_data = {"stream_id": stream_id, "frame": frame}
                            msg_frame = Message(Message.MM, src, json.dumps(msg_data))
                            sckt.sendto(msg_frame.serialize(), (vizinho, SENDER_PORT))
                    except Exception as e:
                        print(f"Error sending frame for {stream_id}: {e}")
            
            time.sleep(0.03333)
        except Exception as e:
            print(f"Error in sender: {e}")
    sckt.close()

def cntrl(sdb:ServerDataBase):
    print("Control thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', ROUTERS_RECEIVER_PORT))
    sckt.listen(10)

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
                                    sdb.inicializaVizinho(msgr_ip)
                                    msg_resp = Message(Message.RESP_NEIGHBOUR, sdb.get_my_ip(msgr_ip), "")
                                    conn.sendall(msg_resp.serialize() + b'\n')
                                elif typeOfMsg == Message.RESP_NEIGHBOUR:
                                    sdb.inicializaVizinho(msgr_ip)
                except Exception as e:
                    print(f"Error in router connection: {e}")
                finally:
                    conn.close()
            
            threading.Thread(target=handle_router_connection, daemon=True).start()
        except Exception as e:
            print(f"Error in control listener: {e}")
    sckt.close()


def metric_updater(sdb:ServerDataBase):
    while True:
        time.sleep(0.5)
        try:
            streams_viz = sdb.get_streams_vizinhos()
            for stream_id, vizinhos in streams_viz.items():
                metric = sdb.calculate_metric(stream_id, vizinhos)
                for viz in vizinhos:
                    sdb.update_metrics(stream_id, metric, viz)
        except Exception as e:
            print("Error in metric updater: ", e)
            break


def main():
    if len(sys.argv) < 2:
        print("Usage: python server.py <server_name>")
        sys.exit(1)
        
    sdb = ServerDataBase(sys.argv[1])
    print(f"Server {sys.argv[1]} started.\n")
    
    thread_listen = threading.Thread(target=listener, args=(sdb,))
    thread_sender = threading.Thread(target=sender, args=(sdb,))
    thread_cntrl = threading.Thread(target=cntrl, args=(sdb,))

    all_threads = [thread_listen, thread_sender, thread_cntrl]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

if __name__ == '__main__':
    main()
