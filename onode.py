import socket
import sys
import threading
import queue
from BD import DataBase
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332
ROUTERS_PORT = 40333


def listener(db:DataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sckt.bind(('', RECEIVER_PORT))
    while True:
        try:
            dados, addr = sckt.recvfrom(1024)
            msg = Message.deserialize(dados)
            db.processMessage(msg, addr)
        except:



def sender(db:DataBase):
    print("Sender thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:

        except:

            

def cntrl(db:DataBase):
    """
    avisa os vizinhos, que está ligado, verifica quais estão e quando um é desligado ele percebe com o tempo
    """
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        try:

        except:


def main():
    sys.stdout.write(f"\033]0;{socket.gethostname()}\007")
    sys.stdout.flush()

    if len(sys.argv) < 2:
        sys.exit(1)
    db = DataBase(sys.argv[1])

    thread_listen = threading.Thread(target=listener, args=(db,))
    thread_sender = threading.Thread(target=sender, args=(db,))

    all_threads = [thread_listen, thread_sender]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

    cntrl(db)

    


if __name__ == '__main__':
    main()
