import threading
import socket
from server_database import ServerDataBase as sdb
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_RECEIVER_PORT = 40333
ROUTERS_SENDER_PORT = 40334




def stream_request_handler(msg, database: sdb):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.initiate_stream(src, stream_id)



def stream_stop_handler(msg, database: sdb):
    src = msg.getSrc()
    stream_id = msg.getData()
    database.end_stream(src, stream_id)




# =============================================================
#                      ROUTER THREADS
# =============================================================

def listener(sdb:sdb):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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

def sender(sdb:sdb):
    pass


def cntrl(sdb:sdb):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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


def main():
    sdb = sdb()
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
