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



def stream_stop_handler(msg, database: ServerDataBase):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.end_stream(src, stream_id)




# =============================================================
#                      ROUTER THREADS
# =============================================================

def listener(sdb:ServerDataBase):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', RECEIVER_PORT))

    while True:
        try:
            data, addr = sckt.recvfrom(4096)
            msg = Message.deserialize(data)
            msgr_ip = msg.getSrc()
            typeOfMsg = msg.getType()

            if typeOfMsg == Message.STREAM_REQUEST:
                threading.Thread(target=stream_request_handler, args=(msg, sdb)).start()
            elif typeOfMsg == Message.STREAM_STOP:
                threading.Thread(target=stream_stop_handler, args=(msg, sdb)).start()
        except Exception as e:
            print("Error in listener: ", e)
            break
    sckt.close()

def sender(sdb:ServerDataBase):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    streams_active = {}
    while True:
        try:
            for stream_id, viz in sdb.stream_vizinhos.items():
                if stream_id not in streams_active:
                    path = sdb.server_streams.get(stream_id) #vai buscar caminho do video da stream a transmitir
                    streams_active[stream_id] = VideoStream(path)

                vs = streams_active.get(stream_id)
                frame = vs.nextFrame()

                for vizinho in viz:
                    src = sdb.get_my_ip(vizinho)
                    #Tranformação da data em string
                    msg_data = {"stream_id": stream_id, "frame": frame}
                    #Construção da mensagem
                    msg_frame = Message(Message.MM, src, json.dumps(msg_data))

                    sckt.sendto(msg_frame.serialize(), (vizinho, SENDER_PORT))
            time.sleep(0.03333)
        except Exception as e:
            print("Error in listener: ", e)
            break
    sckt.close()   


def cntrl(sdb:ServerDataBase):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sckt.bind(('', ROUTERS_RECEIVER_PORT))

    while True:
        try:
            data, addr = sckt.recvfrom(4096)
            msg = Message.deserialize(data)
            msgr_ip = msg.getSrc()
            typeOfMsg = msg.getType() 
            if typeOfMsg == Message.ADD_NEIGHBOUR:
                threading.Thread(target=sdb.inicializaVizinho, args=(msgr_ip,)).start()
                msg_resp = Message(Message.RESP_NEIGHBOUR, sdb.get_my_ip(msgr_ip), "")
                sckt.sendto(msg_resp.serialize(), (msgr_ip, ROUTERS_RECEIVER_PORT))
            elif typeOfMsg == Message.RESP_NEIGHBOUR:
                threading.Thread(target=sdb.inicializaVizinho, args=(msgr_ip,)).start()
        except Exception as e:
            print("Error in listener: ", e)
            break
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
    sdb = ServerDataBase(sys.argv[1])
    print("Server started.\n")
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
