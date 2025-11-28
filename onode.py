import socket
import sys
import threading
import queue
from database import DataBase
from server_database import DataBase as sdb
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334

send_queue = queue.Queue()

def send_message(msg:Message, host:str, port:int):
    send_queue.put((msg, host, port))

def stream_pls_handler(msg:Message, db:DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    need_stream = db.activateStream(ip_viz, stream_id)
    print(f"Processed STREAM_PLS for stream {stream_id} from {msg.getSrc()}.\n")
    if need_stream:
        threading.Thread(target=stream_request_handler, args=(stream_id, db)).start()

def stream_no_handler(msg:Message, db:DataBase):
    stream_id = msg.data
    ip_viz = msg.getSrc()
    is_active = db.deactivateStream(ip_viz, stream_id)
    print(f"Processed VIDEO_NO for stream {stream_id} from {msg.getSrc()}.\n")
    if not is_active:
        threading.Thread(target=stream_stop_handler, args=(stream_id, db)).start()

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
    streams = sdb.get_streams() # depois tirar o sbd 
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


# =============================================================
#                      PRINCIPAL THREADS
# =============================================================

def listener(db:DataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.bind(('', RECEIVER_PORT))
    while True:
        try:
            dados, addr = sckt.recvfrom(1024)
            msg = Message.deserialize(dados)
            typeOfMsg = msg.getType() 
            if typeOfMsg == Message.STREAM_REQUEST:
                threading.Thread(target=stream_pls_handler, args=(msg, db)).start()
            elif typeOfMsg == Message.STREAM_STOP:
                threading.Thread(target=stream_no_handler, args=(msg, db)).start()
            elif typeOfMsg == Message.STREAMS_AVAILABLE:
                threading.Thread(target=streams_available_handler, args=(msg, db)).start()
        except Exception as e:
            print("Error in listener: ", e)
            break
    sckt.close()



def sender(db:DataBase):
    print("Sender thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:
            msg, host, port = send_queue.get()
            try:
                sckt.sendto(msg.serialize(), (host, port))
            except Exception:
                # on error try with a fresh socket
                sckt.close()
                sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sckt.sendto(msg.serialize(), (host, port))
        except Exception as e:
            print("Error in sender: ", e)
            break
    sckt.close()
            

def cntrl(db:DataBase):
    """
    avisa os vizinhos, que está ligado, verifica quais estão e quando um é desligado ele percebe com o tempo
    """
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.bind(('', ROUTERS_RECEIVER_PORT))
    vizinhos = db.get_vizinhos()
    threading.Thread(target=wake_router_handler, args=(vizinhos, db)).start()
    while True:
        try:
            dados, addr = sckt.recvfrom(1024)
            msg = Message.deserialize(dados)
            msgr_ip = msg.getSrc()
            typeOfMsg = msg.getType() 
            if typeOfMsg == Message.ADD_NEIGHBOUR:
                db.inicializaVizinho(msgr_ip)
                msg_resp = Message(Message.RESP_NEIGHBOUR, db.get_my_ip(msgr_ip), "")
                sckt.sendto(msg_resp.serialize(), (msgr_ip, ROUTERS_RECEIVER_PORT))
            elif typeOfMsg == Message.RESP_NEIGHBOUR:
                db.inicializaVizinho(msgr_ip)
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
