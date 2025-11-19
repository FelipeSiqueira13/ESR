import socket
import sys
import threading
import queue
from BD import DataBase
from msg import Message

RECEIVER_PORT = 40331
SENDER_PORT = 40332


def listener(db:DataBase):
    print("Listener thread started")
    sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    msg = Message(Mensagem.ALIVE_RECEPTOR).serialize()
    interval = 20
    while True:
        try:


def main():
    sys.stdout.write(f"\033]0;{socket.gethostname()}\007")
    sys.stdout.flush()

    if len(sys.argv) < 2:
        sys.exit(1)
    db = DataBase(sys.argv[1])

    thread_listen = threading.Thread(target=, args=(db,))
    thread_sender = threading.Thread(target=, args=(db,))

    all_threads = [thread_listen, thread_sender]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

    


if __name__ == '__main__':
    main()
